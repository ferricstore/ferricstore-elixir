defmodule FerricStore.Flow.QueryResponse.Indexes do
  @moduledoc false

  alias FerricStore.Flow.{
    QueryIndex,
    QueryIndexBuild,
    QueryIndexCoverage,
    QueryIndexField,
    QueryIndexFormat,
    QueryIndexRetirement,
    QueryIndexServices,
    QueryIndexStatistics,
    QueryIndexStatus,
    QueryIndexValidation
  }

  alias FerricStore.Flow.QueryResponse.Validation, as: V
  alias FerricStore.Flow.QueryResponse.IndexContract
  alias FerricStore.Types

  @contract "ferric.flow.query.indexes/v1"
  @build_phases ~w(pending snapshot backfill done)
  @validation_phases ~w(pending source index counter cleanup done)
  @retirement_phases ~w(pending fence index counter reverse cleanup done)
  @maximum_unsigned_64 18_446_744_073_709_551_615

  def decode(value, expected_id \\ nil)

  def decode(value, expected_id) when is_map(value) do
    with {:ok, @contract} <- V.contract(value, "contract_version", @contract),
         {:ok, observed} <- V.unsigned(value, "observed_at_ms"),
         {:ok, max_age} <- V.unsigned(value, "statistics_max_age_ms"),
         {:ok, registry} <- registry(Types.get(value, "registry")),
         {:ok, services} <- services(Types.get(value, "services")),
         {:ok, indexes} <- entries(Types.get(value, "indexes")),
         status = %QueryIndexStatus{
           contract_version: @contract,
           observed_at_ms: observed,
           statistics_max_age_ms: max_age,
           registry: registry,
           services: services,
           indexes: indexes,
           raw: value
         },
         :ok <- IndexContract.validate(status, expected_id) do
      {:ok, status}
    end
  end

  def decode(value, _expected_id), do: V.invalid(:indexes, value)

  defp registry(value) when is_map(value) do
    with {:ok, epoch} <- V.unsigned(value, "epoch"),
         {:ok, catalog_version} <- V.positive_unsigned(value, "catalog_version"),
         do: {:ok, %{epoch: epoch, catalog_version: catalog_version}}
  end

  defp registry(value), do: V.invalid(:registry, value)

  defp services(value) when is_map(value) do
    with {:ok, registry} <- choice(value, "registry", ~w(ready unavailable), :services),
         {:ok, lifecycle_worker} <-
           choice(value, "lifecycle_worker", ~w(ready unavailable), :services),
         {:ok, statistics_store} <-
           choice(value, "statistics_store", ~w(ready unavailable), :services),
         {:ok, statistics_worker} <-
           choice(value, "statistics_worker", ~w(ready unavailable), :services) do
      {:ok,
       %QueryIndexServices{
         registry: registry,
         lifecycle_worker: lifecycle_worker,
         statistics_store: statistics_store,
         statistics_worker: statistics_worker,
         raw: value
       }}
    end
  end

  defp services(value), do: V.invalid(:services, value)

  defp entries(entries) when is_list(entries) and length(entries) <= 32 do
    entries
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {entry, index}, {:ok, acc} ->
      case entry(entry) do
        {:ok, decoded} -> {:cont, {:ok, [decoded | acc]}}
        {:error, reason} -> {:halt, V.invalid({:index, index}, reason)}
      end
    end)
    |> then(fn
      {:ok, values} -> {:ok, Enum.reverse(values)}
      error -> error
    end)
  end

  defp entries(value), do: V.invalid(:indexes, value)

  defp entry(value) when is_map(value) do
    with {:ok, id} <- V.bounded_binary(value, "id", 64),
         {:ok, version} <- V.positive_unsigned(value, "version"),
         {:ok, build_id} <- V.bounded_binary(value, "build_id", 128),
         {:ok, source} <- choice(value, "source", ["runs"], :source),
         {:ok, state} <-
           choice(value, "state", ~w(building validating active retiring failed), :state),
         {:ok, queryable} <- V.required_boolean(value, "queryable"),
         {:ok, fields} <- fields(Types.get(value, "fields")),
         {:ok, workloads} <- unique_texts(Types.get(value, "workloads"), 16, 64, :workloads),
         {:ok, count_prefixes} <-
           count_prefixes(Types.get(value, "count_prefixes"), length(fields)),
         {:ok, covering_fields} <-
           unique_texts(Types.get(value, "covering_fields"), 32, 512, :covering_fields),
         {:ok, format} <- format(Types.get(value, "format")),
         {:ok, coverage} <- coverage(Types.get(value, "coverage")),
         {:ok, build} <- build(Types.get(value, "build")),
         {:ok, validation} <- validation(Types.get(value, "validation")),
         {:ok, retirement} <- retirement(Types.get(value, "retirement")),
         {:ok, statistics} <- statistics(Types.get(value, "statistics")) do
      {:ok,
       %QueryIndex{
         id: id,
         version: version,
         build_id: build_id,
         source: source,
         state: state,
         queryable: queryable,
         fields: fields,
         workloads: workloads,
         count_prefixes: count_prefixes,
         covering_fields: covering_fields,
         format: format,
         coverage: coverage,
         build: build,
         validation: validation,
         retirement: retirement,
         statistics: statistics,
         raw: value
       }}
    end
  end

  defp entry(value), do: V.invalid(:index, value)

  defp fields(value) when is_list(value) and length(value) in 2..8 do
    value
    |> Enum.reduce_while({:ok, {[], MapSet.new()}}, fn field, {:ok, {acc, seen}} ->
      with true <- is_map(field),
           {:ok, name} <- V.bounded_binary(field, "name", 512),
           false <- MapSet.member?(seen, name),
           {:ok, direction} <- choice(field, "direction", ~w(asc desc), :field_direction),
           {:ok, encoding} <- choice(field, "encoding", ~w(hashed ordered), :field_encoding) do
        decoded = %QueryIndexField{
          name: name,
          direction: direction,
          encoding: encoding,
          raw: field
        }

        {:cont, {:ok, {[decoded | acc], MapSet.put(seen, name)}}}
      else
        _invalid -> {:halt, V.invalid(:fields, value)}
      end
    end)
    |> then(fn
      {:ok, {decoded, _seen}} -> {:ok, Enum.reverse(decoded)}
      error -> error
    end)
  end

  defp fields(value), do: V.invalid(:fields, value)

  defp count_prefixes(value, field_count) when is_list(value) and length(value) <= field_count do
    if Enum.all?(value, &(is_integer(&1) and &1 > 0 and &1 <= field_count)) and
         value == Enum.sort(Enum.uniq(value)),
       do: {:ok, value},
       else: V.invalid(:count_prefixes, value)
  end

  defp count_prefixes(value, _field_count), do: V.invalid(:count_prefixes, value)

  defp format(value) when is_map(value) do
    with {:ok, query_row} <- V.bounded_binary(value, "query_row", 128),
         {:ok, key} <- V.bounded_binary(value, "key", 128),
         {:ok, entry} <- V.bounded_binary(value, "entry", 128),
         {:ok, reverse} <- V.bounded_binary(value, "reverse", 128),
         {:ok, counter} <- nullable_text(value, "counter", 128) do
      {:ok,
       %QueryIndexFormat{
         query_row: query_row,
         key: key,
         entry: entry,
         reverse: reverse,
         counter: counter,
         raw: value
       }}
    end
  end

  defp format(value), do: V.invalid(:format, value)

  defp coverage(value) when is_map(value) do
    with {:ok, complete} <- V.unsigned(value, "complete_shards"),
         {:ok, total} <- V.positive_unsigned(value, "total_shards"),
         true <- complete <= total,
         {:ok, validation} <-
           choice(value, "validation", ~w(pending passed failed), :coverage_validation) do
      {:ok,
       %QueryIndexCoverage{
         complete_shards: complete,
         total_shards: total,
         validation: validation,
         raw: value
       }}
    else
      false -> V.invalid(:coverage, value)
      {:error, _reason} = error -> error
    end
  end

  defp coverage(value), do: V.invalid(:coverage, value)

  defp build(value) do
    with {:ok, progress} <- progress(value, :build, @build_phases),
         {:ok, scanned_records} <- V.unsigned(value, "scanned_records"),
         {:ok, written_entries} <- V.unsigned(value, "written_entries"),
         {:ok, written_bytes} <- V.unsigned(value, "written_bytes") do
      {:ok,
       struct!(
         QueryIndexBuild,
         Map.merge(progress, %{
           scanned_records: scanned_records,
           written_entries: written_entries,
           written_bytes: written_bytes
         })
       )}
    end
  end

  defp validation(value) do
    with {:ok, progress} <- progress(value, :validation, @validation_phases),
         {:ok, status} <- choice(value, "status", ~w(pending passed failed), :validation_status),
         {:ok, checked_records} <- V.unsigned(value, "checked_records"),
         {:ok, checked_entries} <- V.unsigned(value, "checked_entries"),
         {:ok, mismatches} <- V.unsigned(value, "mismatches"),
         {:ok, failure_reason} <- nullable_text(value, "failure_reason", 128),
         {:ok, validated_at_ms} <- nullable_unsigned(value, "validated_at_ms") do
      {:ok,
       struct!(
         QueryIndexValidation,
         Map.merge(progress, %{
           status: status,
           checked_records: checked_records,
           checked_entries: checked_entries,
           mismatches: mismatches,
           failure_reason: failure_reason,
           validated_at_ms: validated_at_ms
         })
       )}
    end
  end

  defp retirement(value) when is_map(value) do
    with {:ok, status} <-
           choice(value, "status", ~w(not_applicable pending complete), :retirement_status) do
      if status == "not_applicable" do
        {:ok,
         %QueryIndexRetirement{
           status: status,
           phase_counts: nil,
           current_phases: nil,
           completed_shards: nil,
           total_shards: nil,
           deleted_entries: nil,
           deleted_bytes: nil,
           rewritten_reverse_rows: nil,
           raw: value
         }}
      else
        retirement_progress(value, status)
      end
    end
  end

  defp retirement(value), do: V.invalid(:retirement, value)

  defp retirement_progress(value, status) do
    with {:ok, phase_counts} <- phase_counts(Types.get(value, "phase_counts"), :retirement),
         {:ok, current_phases} <-
           phases(Types.get(value, "current_phases"), :retirement, @retirement_phases),
         {:ok, completed_shards} <- V.unsigned(value, "completed_shards"),
         {:ok, total_shards} <- V.positive_unsigned(value, "total_shards"),
         true <- completed_shards <= total_shards,
         {:ok, deleted_entries} <- V.unsigned(value, "deleted_entries"),
         {:ok, deleted_bytes} <- V.unsigned(value, "deleted_bytes"),
         {:ok, rewritten_reverse_rows} <- V.unsigned(value, "rewritten_reverse_rows") do
      {:ok,
       %QueryIndexRetirement{
         status: status,
         phase_counts: phase_counts,
         current_phases: current_phases,
         completed_shards: completed_shards,
         total_shards: total_shards,
         deleted_entries: deleted_entries,
         deleted_bytes: deleted_bytes,
         rewritten_reverse_rows: rewritten_reverse_rows,
         raw: value
       }}
    else
      false -> V.invalid(:retirement, value)
      {:error, _reason} = error -> error
    end
  end

  defp statistics(value) when is_map(value) do
    with {:ok, status} <-
           choice(
             value,
             "status",
             ~w(fresh stale future mixed missing unavailable),
             :statistics_status
           ),
         {:ok, samples} <- V.unsigned(value, "samples"),
         {:ok, fresh_samples} <- V.unsigned(value, "fresh_samples"),
         {:ok, stale_samples} <- V.unsigned(value, "stale_samples"),
         {:ok, future_samples} <- V.unsigned(value, "future_samples"),
         true <- fresh_samples + stale_samples == samples and future_samples <= stale_samples,
         {:ok, oldest_collected_at_ms} <- nullable_unsigned(value, "oldest_collected_at_ms"),
         {:ok, newest_collected_at_ms} <- nullable_unsigned(value, "newest_collected_at_ms"),
         {:ok, oldest_age_ms} <- nullable_unsigned(value, "oldest_age_ms"),
         {:ok, newest_age_ms} <- nullable_unsigned(value, "newest_age_ms") do
      {:ok,
       %QueryIndexStatistics{
         status: status,
         samples: samples,
         fresh_samples: fresh_samples,
         stale_samples: stale_samples,
         future_samples: future_samples,
         oldest_collected_at_ms: oldest_collected_at_ms,
         newest_collected_at_ms: newest_collected_at_ms,
         oldest_age_ms: oldest_age_ms,
         newest_age_ms: newest_age_ms,
         raw: value
       }}
    else
      false -> V.invalid(:statistics, value)
      {:error, _reason} = error -> error
    end
  end

  defp statistics(value), do: V.invalid(:statistics, value)

  defp progress(value, section, allowed_phases) when is_map(value) do
    with {:ok, scope} <- choice(value, "scope", ["catalog_build"], {section, :scope}),
         {:ok, phase_counts} <- phase_counts(Types.get(value, "phase_counts"), section),
         {:ok, current_phases} <-
           phases(Types.get(value, "current_phases"), section, allowed_phases),
         {:ok, completed_shards} <- V.unsigned(value, "completed_shards"),
         {:ok, total_shards} <- V.positive_unsigned(value, "total_shards"),
         true <- completed_shards <= total_shards do
      {:ok,
       %{
         scope: scope,
         phase_counts: phase_counts,
         current_phases: current_phases,
         completed_shards: completed_shards,
         total_shards: total_shards,
         raw: value
       }}
    else
      false -> V.invalid(section, value)
      {:error, _reason} = error -> error
    end
  end

  defp progress(value, section, _allowed), do: V.invalid(section, value)

  defp phase_counts(value, section) when is_map(value) and map_size(value) <= 16 do
    Enum.reduce_while(value, {:ok, %{}}, fn {phase, count}, {:ok, acc} ->
      if is_binary(phase) and phase != "" and byte_size(phase) <= 64 and String.valid?(phase) and
           is_integer(count) and count >= 0 and count <= @maximum_unsigned_64 do
        {:cont, {:ok, Map.put(acc, phase, count)}}
      else
        {:halt, V.invalid({section, :phase_counts}, value)}
      end
    end)
  end

  defp phase_counts(value, section), do: V.invalid({section, :phase_counts}, value)

  defp phases(value, section, allowed) do
    with {:ok, phases} <- unique_texts(value, length(allowed), 64, {section, :current_phases}),
         true <- Enum.all?(phases, &(&1 in allowed)) do
      {:ok, phases}
    else
      false -> V.invalid({section, :current_phases}, value)
      {:error, _reason} = error -> error
    end
  end

  defp unique_texts(value, maximum, maximum_bytes, field)
       when is_list(value) and length(value) <= maximum do
    with true <-
           Enum.all?(value, fn text ->
             is_binary(text) and text != "" and byte_size(text) <= maximum_bytes and
               String.valid?(text)
           end),
         true <- length(value) == MapSet.size(MapSet.new(value)) do
      {:ok, value}
    else
      false -> V.invalid(field, value)
    end
  end

  defp unique_texts(value, _maximum, _maximum_bytes, field), do: V.invalid(field, value)

  defp nullable_text(value, field, maximum_bytes) do
    if V.has_key?(value, field) do
      case Types.get(value, field) do
        nil -> {:ok, nil}
        _present -> V.bounded_binary(value, field, maximum_bytes)
      end
    else
      V.invalid({:nullable, field}, value)
    end
  end

  defp nullable_unsigned(value, field) do
    if V.has_key?(value, field) do
      case Types.get(value, field) do
        nil -> {:ok, nil}
        _present -> V.unsigned(value, field)
      end
    else
      V.invalid({:nullable, field}, value)
    end
  end

  defp choice(value, field, choices, error_field) do
    actual = Types.get(value, field)
    if actual in choices, do: {:ok, actual}, else: V.invalid(error_field, actual)
  end
end
