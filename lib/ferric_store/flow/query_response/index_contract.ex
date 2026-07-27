defmodule FerricStore.Flow.QueryResponse.IndexContract do
  @moduledoc false

  alias FerricStore.Flow.QueryResponse.Validation, as: V

  @build_phases ~w(pending snapshot backfill done)
  @validation_phases ~w(pending source index counter cleanup done)
  @retirement_phases ~w(pending fence index counter reverse cleanup done)
  @identifier ~r/\A[A-Za-z0-9_.:-]+\z/
  @unquoted_metadata ~r/\A[A-Za-z0-9_-]+\z/
  @attribute_selector ~r/\Aattribute\['((?:[^']|'')*)'\]\z/u
  @state_meta_selector ~r/\Astate_meta\['((?:[^']|'')*)'\]\['((?:[^']|'')*)'\]\z/u
  @integer_fields MapSet.new(~w(
                    version priority created_at_ms updated_at_ms next_run_at_ms
                    lease_deadline_ms attempts max_active_ms
                  ))
  @keyword_fields MapSet.new(~w(
                    partition_key run_id event_id type state run_state parent_flow_id
                    root_flow_id correlation_id
                  ))
  @retirement_progress ~w(
                         phase_counts current_phases completed_shards total_shards
                         deleted_entries deleted_bytes rewritten_reverse_rows
                       )

  def validate(status, expected_id \\ nil) do
    identities = Enum.map(status.indexes, &{&1.id, &1.version})

    cond do
      identities != Enum.sort(Enum.uniq(identities)) ->
        fail(status, :identity_order)

      expected_id != nil and
          (identities == [] or Enum.any?(status.indexes, &(&1.id != expected_id))) ->
        fail(status, :filtered_identity)

      true ->
        with :ok <- validate_indexes(status),
             :ok <- validate_statistics_service(status) do
          :ok
        end
    end
  end

  defp validate_indexes(status) do
    Enum.reduce_while(status.indexes, :ok, fn index, :ok ->
      case validate_index(index, status) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp validate_index(index, status) do
    with true <- valid_identifier?(index.id),
         true <- Enum.all?(index.workloads, &valid_identifier?/1),
         :ok <- validate_fields(index),
         :ok <- validate_covering(index),
         true <- index.count_prefixes != [] == not is_nil(index.format.counter),
         :ok <- validate_progress(index.build, @build_phases, :build),
         :ok <- validate_progress(index.validation, @validation_phases, :validation),
         :ok <- validate_retirement(index.retirement),
         :ok <- validate_shards(index),
         :ok <- validate_lifecycle(index),
         :ok <- validate_validation(index.validation),
         :ok <- validate_statistics(index.statistics, status) do
      :ok
    else
      false -> fail(status, :index_contract)
      {:error, _reason} = error -> error
    end
  end

  defp validate_fields(index) do
    [first | _rest] = index.fields

    with true <-
           {first.name, first.direction, first.encoding} ==
             {"partition_key", "asc", "hashed"},
         true <- Enum.all?(index.fields, &(&1.encoding != "hashed" or &1.direction == "asc")),
         kinds = Enum.map(index.fields, &field_kind(&1.name)),
         true <- Enum.all?(kinds, &(&1 != nil)),
         true <-
           Enum.zip(index.fields, kinds)
           |> Enum.all?(fn {field, kind} -> field.encoding != "ordered" or kind == :integer end),
         true <- Enum.count(kinds, &(&1 == :attribute)) <= 1,
         true <-
           Enum.all?(index.count_prefixes, fn prefix ->
             index.fields |> Enum.take(prefix) |> Enum.all?(&(&1.encoding == "hashed"))
           end) do
      :ok
    else
      false -> V.invalid(:index_fields, index.raw)
    end
  end

  defp validate_covering(%{covering_fields: []}), do: :ok

  defp validate_covering(index) do
    covering = MapSet.new(index.covering_fields)
    required = MapSet.new(["run_id", "version" | Enum.map(index.fields, & &1.name)])

    if Enum.all?(index.covering_fields, &(field_kind(&1) != nil)) and
         MapSet.subset?(required, covering),
       do: :ok,
       else: V.invalid(:index_covering_fields, index.raw)
  end

  defp validate_progress(progress, phases, section) do
    counts = progress.phase_counts
    expected_current = Enum.filter(phases, &Map.has_key?(counts, &1))

    if map_size(counts) > 0 and
         Enum.all?(counts, fn {phase, count} -> phase in phases and count > 0 end) and
         Enum.sum(Map.values(counts)) == progress.total_shards and
         progress.current_phases == expected_current and
         progress.completed_shards == Map.get(counts, "done", 0),
       do: :ok,
       else: V.invalid({section, :progress}, progress.raw)
  end

  defp validate_retirement(%{status: "not_applicable"} = retirement) do
    if Enum.any?(@retirement_progress, &V.has_key?(retirement.raw, &1)),
      do: V.invalid(:retirement_progress, retirement.raw),
      else: :ok
  end

  defp validate_retirement(retirement),
    do: validate_progress(retirement, @retirement_phases, :retirement)

  defp validate_shards(index) do
    totals = [
      index.coverage.total_shards,
      index.build.total_shards,
      index.validation.total_shards
    ]

    totals =
      if index.retirement.total_shards == nil,
        do: totals,
        else: [index.retirement.total_shards | totals]

    queryable =
      index.state == "active" and
        index.coverage.complete_shards == index.coverage.total_shards and
        index.coverage.validation == "passed"

    if length(Enum.uniq(totals)) == 1 and
         index.coverage.complete_shards == index.build.completed_shards and
         index.coverage.validation == index.validation.status and index.queryable == queryable,
       do: :ok,
       else: V.invalid(:index_shards, index.raw)
  end

  defp validate_lifecycle(index) do
    built = index.build.completed_shards == index.build.total_shards
    validation = index.validation.status
    retirement = index.retirement.status

    valid =
      case index.state do
        "building" ->
          not built and validation == "pending" and retirement == "not_applicable"

        "validating" ->
          built and validation in ~w(pending passed) and retirement == "not_applicable"

        "active" ->
          built and validation == "passed" and retirement == "not_applicable"

        "retiring" ->
          built and validation in ~w(passed failed) and retirement in ~w(pending complete)

        "failed" ->
          validation in ~w(passed failed) and retirement in ~w(pending complete)
      end

    if valid, do: :ok, else: V.invalid(:index_lifecycle, index.raw)
  end

  defp validate_validation(validation) do
    valid =
      case validation.status do
        "pending" ->
          validation.mismatches == 0 and validation.failure_reason == nil and
            validation.validated_at_ms == nil

        "passed" ->
          validation.mismatches == 0 and validation.failure_reason == nil and
            validation.validated_at_ms != nil

        "failed" ->
          validation.mismatches > 0 and is_binary(validation.failure_reason) and
            validation.failure_reason != "" and validation.validated_at_ms != nil
      end

    if valid, do: :ok, else: V.invalid(:index_validation, validation.raw)
  end

  defp validate_statistics(statistics, status) do
    timestamps = [
      statistics.oldest_collected_at_ms,
      statistics.newest_collected_at_ms,
      statistics.oldest_age_ms,
      statistics.newest_age_ms
    ]

    cond do
      statistics.samples == 0 ->
        if statistics.status in ~w(missing unavailable) and Enum.all?(timestamps, &is_nil/1),
          do: :ok,
          else: V.invalid(:index_statistics, statistics.raw)

      Enum.any?(timestamps, &is_nil/1) ->
        V.invalid(:index_statistics, statistics.raw)

      true ->
        [oldest, newest, oldest_age, newest_age] = timestamps

        expected =
          cond do
            statistics.fresh_samples == statistics.samples -> ["fresh"]
            statistics.fresh_samples == 0 and statistics.future_samples > 0 -> ~w(stale future)
            statistics.fresh_samples == 0 -> ["stale"]
            true -> ["mixed"]
          end

        if oldest <= newest and oldest_age == max(status.observed_at_ms - oldest, 0) and
             newest_age == max(status.observed_at_ms - newest, 0) and
             statistics.status in expected,
           do: :ok,
           else: V.invalid(:index_statistics, statistics.raw)
    end
  end

  defp validate_statistics_service(status) do
    statuses = Enum.map(status.indexes, & &1.statistics.status)

    valid =
      if status.services.statistics_store == "unavailable",
        do: Enum.all?(statuses, &(&1 == "unavailable")),
        else: Enum.all?(statuses, &(&1 != "unavailable"))

    if valid, do: :ok, else: fail(status, :statistics_service)
  end

  defp field_kind(name) do
    cond do
      MapSet.member?(@integer_fields, name) -> :integer
      MapSet.member?(@keyword_fields, name) -> :keyword
      match_unquoted?(name, "attribute", 1) -> :attribute
      match_unquoted?(name, "state_meta", 2) -> :state_meta
      match_attribute_selector?(name) -> :attribute
      match_state_meta_selector?(name) -> :state_meta
      true -> nil
    end
  end

  defp match_unquoted?(name, root, count) do
    case String.split(name, ".") do
      [^root | segments] when length(segments) == count -> Enum.all?(segments, &valid_unquoted?/1)
      _other -> false
    end
  end

  defp match_attribute_selector?(name) do
    case Regex.run(@attribute_selector, name, capture: :all_but_first) do
      [segment] ->
        decoded = String.replace(segment, "''", "'")
        valid_metadata?(decoded, true) and name == external_selector("attribute", [decoded])

      _other ->
        false
    end
  end

  defp match_state_meta_selector?(name) do
    case Regex.run(@state_meta_selector, name, capture: :all_but_first) do
      [state, field] ->
        decoded = Enum.map([state, field], &String.replace(&1, "''", "'"))

        valid_metadata?(Enum.at(decoded, 0), false) and
          valid_metadata?(Enum.at(decoded, 1), true) and
          name == external_selector("state_meta", decoded)

      _other ->
        false
    end
  end

  defp valid_unquoted?(value) do
    value != "" and byte_size(value) <= 64 and not String.starts_with?(value, "__") and
      Regex.match?(@unquoted_metadata, value)
  end

  defp valid_metadata?(value, reject_reserved) do
    value != "" and String.valid?(value) and byte_size(value) <= 64 and
      (not reject_reserved or not String.starts_with?(value, "__"))
  end

  defp external_selector(root, segments) do
    if Enum.all?(segments, &valid_unquoted?/1) do
      Enum.join([root | segments], ".")
    else
      root <> Enum.map_join(segments, "", &("['" <> String.replace(&1, "'", "''") <> "']"))
    end
  end

  defp valid_identifier?(value),
    do: is_binary(value) and byte_size(value) in 1..64 and Regex.match?(@identifier, value)

  defp fail(status, reason), do: V.invalid({:index_contract, reason}, status.raw)
end
