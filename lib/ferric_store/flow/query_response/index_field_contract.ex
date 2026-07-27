defmodule FerricStore.Flow.QueryResponse.IndexFieldContract do
  @moduledoc false

  alias FerricStore.Flow.QueryResponse.Validation, as: V

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

  def validate(index) do
    with :ok <- validate_fields(index), do: validate_covering(index)
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
end
