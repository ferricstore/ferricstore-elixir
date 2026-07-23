defmodule FerricStore.SDK.Native.FlowQueryContract do
  @moduledoc false

  alias FerricStore.Types

  @request_contract "ferric.flow.query.request/v1"
  @result_contract "ferric.flow.query.result/v1"
  @explain_contract "ferric.flow.explain/v1"
  @index_status_contract "ferric.flow.query.indexes/v1"
  @required_capabilities ~w(
    flow_query_v1
    flow_explain_v1
    flow_explain_analyze_v1
    flow_composite_index_v1
    flow_query_index_status_v1
  )
  @required_shapes ~w(
    runs_by_run_id_record
    runs_by_partition_and_run_id_record
    runs_by_partition_predicates_ordered_records
    runs_by_partition_type_state_ordered_records
    runs_by_partition_type_terminals_ordered_records
    runs_by_partition_metadata_ordered_records
    runs_by_partition_type_running_lease_deadline_ordered_records
    runs_by_partition_parent_ordered_records
    runs_by_partition_root_ordered_records
    runs_by_partition_correlation_ordered_records
    runs_by_partition_predicates_count
    events_by_run_id_ordered_records
  )

  @spec validate(map()) :: :ok | {:error, map()}
  def validate(capabilities) when is_map(capabilities) do
    case Types.get(capabilities, "flow_query") do
      manifest when is_map(manifest) -> validate_manifest(manifest)
      _missing -> {:error, %{missing_capability: "flow_query"}}
    end
  end

  def validate(_capabilities), do: {:error, %{missing_capability: "flow_query"}}

  defp validate_manifest(manifest) do
    with :ok <- exact(manifest, "request_contract", @request_contract),
         :ok <- exact(manifest, "result_contract", @result_contract),
         :ok <- exact(manifest, "explain_contract", @explain_contract),
         :ok <- exact(manifest, "index_status_contract", @index_status_contract),
         :ok <- includes(manifest, "capabilities", @required_capabilities),
         :ok <- includes(manifest, "language_versions", ["FQL1"]) do
      includes(manifest, "shapes", @required_shapes)
    end
  end

  defp exact(manifest, field, expected) do
    case Types.get(manifest, field) do
      ^expected -> :ok
      actual -> {:error, %{flow_query: field, expected: expected, advertised: actual}}
    end
  end

  defp includes(manifest, field, required) do
    case Types.get(manifest, field) do
      values when is_list(values) and length(values) <= 64 ->
        include_values(values, field, required)

      actual ->
        {:error, %{flow_query: field, advertised: actual}}
    end
  end

  defp include_values(values, field, required) do
    with :ok <- validate_values(values),
         [] <- required -- values do
      :ok
    else
      [_ | _] = missing -> {:error, %{flow_query: field, missing: missing}}
      {:error, _reason} = error -> error
    end
  end

  defp validate_values(values) do
    if Enum.all?(values, &(is_binary(&1) and &1 != "" and byte_size(&1) <= 128)) and
         length(values) == length(Enum.uniq(values)) do
      :ok
    else
      {:error, %{flow_query: :invalid_manifest_list}}
    end
  end
end
