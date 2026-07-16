defmodule FerricStore.FlowRouting do
  @moduledoc false

  alias FerricStore.DeadlineBudget
  alias FerricStore.FlowRouting.{PartitionList, RouteSource}
  alias FerricStore.Protocol.CommandSpec
  alias FerricStore.RouteKey

  @generic_payload_fields [
    {"key", :key},
    {"partition_key", :partition_key},
    {"id", :id},
    {"owner_flow_id", :owner_flow_id},
    {"parent_flow_id", :parent_flow_id},
    {"root_flow_id", :root_flow_id},
    {"correlation_id", :correlation_id},
    {"scope", :scope}
  ]
  @flow_payload_fields [
    {"partition_key", :partition_key},
    {"partition_keys", :partition_keys},
    {"id", :id},
    {"scope", :scope},
    {"owner_flow_id", :owner_flow_id},
    {"name", :name}
  ]

  @type resolution ::
          RouteKey.resolution()
          | {:error, {:batch_too_large, map()}}
          | {:error, {:conflicting_route_fields, [binary()]}}

  @spec resolve_payload(term(), term(), term()) :: resolution()
  def resolve_payload(opcode, payload, opts), do: do_resolve_payload(opcode, payload, opts, nil)

  @spec resolve_payload(term(), term(), term(), DeadlineBudget.t()) ::
          resolution() | {:error, :timeout}
  def resolve_payload(opcode, payload, opts, %DeadlineBudget{} = budget) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         resolution <- do_resolve_payload(opcode, payload, opts, budget),
         :ok <- finish_deadline(resolution, budget) do
      resolution
    end
  end

  defp do_resolve_payload(opcode, payload, opts, budget) do
    case RouteKey.from_options(opts, [:key, :route_key]) do
      :none -> resolve_payload_route(opcode_name(opcode), payload, budget)
      resolution -> resolution
    end
  end

  defp resolve_payload_route("FLOW." <> _rest = command, payload, budget)
       when is_map(payload) do
    with :ok <- RouteKey.ensure_unambiguous_payload_fields(payload, @flow_payload_fields) do
      flow_payload_route(command, payload, budget)
    end
  end

  defp resolve_payload_route(_command, payload, _budget) when is_map(payload),
    do: RouteKey.from_payload(payload, @generic_payload_fields)

  defp resolve_payload_route(_command, _payload, _budget), do: :none

  defp flow_payload_route(command, payload, budget) do
    with :ok <- reject_conflicting_partition_fields(payload) do
      case payload_route_kind(command) do
        :control -> :none
        :approval -> payload |> field("id") |> RouteSource.logical_partition()
        :governance -> payload |> field("scope") |> RouteSource.logical_partition()
        :claim -> claim_payload_route(payload, budget)
        :many -> shared_partition_payload_route(payload)
        :generic -> generic_payload_route(command, payload, budget)
      end
    end
  end

  defp payload_route_kind(command) do
    cond do
      CommandSpec.flow_property?(command, :schedule) -> :control
      CommandSpec.flow_property?(command, :approval) -> :approval
      CommandSpec.flow_property?(command, :governance) -> :governance
      CommandSpec.flow_property?(command, :claim) -> :claim
      CommandSpec.flow_property?(command, :many) -> :many
      true -> :generic
    end
  end

  defp generic_payload_route(command, payload, budget) do
    cond do
      command == "FLOW.VALUE.MGET" ->
        value_mget_payload_route(payload, budget)

      field_present?(payload, "partition_key") ->
        payload |> field("partition_key") |> RouteSource.logical_partition()

      field_present?(payload, "partition_keys") ->
        payload |> field("partition_keys") |> partition_list_resolution(false, budget)

      command == "FLOW.VALUE.PUT" ->
        value_put_payload_route(payload)

      CommandSpec.flow_property?(command, :type_scoped) ->
        :none

      CommandSpec.flow_property?(command, :state_id) ->
        payload |> field("id") |> RouteSource.auto_id()

      true ->
        :none
    end
  end

  defp value_mget_payload_route(payload, budget) do
    case field(payload, "refs") do
      [_ref | _refs] = refs -> resolve_partition_list(refs, &RouteKey.validate/1, budget)
      _empty_or_invalid -> :none
    end
  end

  defp value_put_payload_route(payload) do
    case {field(payload, "owner_flow_id"), field(payload, "name")} do
      {owner, name} when is_binary(owner) and is_binary(name) -> RouteSource.auto_id(owner)
      _missing -> :none
    end
  end

  defp claim_payload_route(payload, budget) do
    cond do
      field_present?(payload, "partition_key") ->
        payload |> field("partition_key") |> RouteSource.claim_partition()

      field_present?(payload, "partition_keys") ->
        payload |> field("partition_keys") |> partition_list_resolution(true, budget)

      true ->
        :none
    end
  end

  defp shared_partition_payload_route(payload) do
    if field_present?(payload, "partition_key") do
      payload |> field("partition_key") |> RouteSource.logical_partition()
    else
      :none
    end
  end

  defp partition_list_resolution(partitions, claim?, budget),
    do: resolve_partition_list(partitions, &partition_resolution(&1, claim?), budget)

  defp resolve_partition_list(partitions, resolver, nil),
    do: PartitionList.resolve(partitions, resolver)

  defp resolve_partition_list(partitions, resolver, budget),
    do: PartitionList.resolve(partitions, resolver, budget)

  defp partition_resolution(partition, true), do: RouteSource.claim_partition(partition)
  defp partition_resolution(partition, false), do: RouteSource.logical_partition(partition)

  defp field(map, name) do
    case Map.fetch(map, name) do
      {:ok, value} -> value
      :error -> Map.get(map, String.to_existing_atom(name))
    end
  rescue
    ArgumentError -> nil
  end

  defp field_present?(map, name) do
    Map.has_key?(map, name) or
      try do
        Map.has_key?(map, String.to_existing_atom(name))
      rescue
        ArgumentError -> false
      end
  end

  defp opcode_name(opcode), do: CommandSpec.name(opcode)

  defp reject_conflicting_partition_fields(payload) do
    if field_present?(payload, "partition_key") and field_present?(payload, "partition_keys") do
      {:error, {:conflicting_route_fields, ["partition_key", "partition_keys"]}}
    else
      :ok
    end
  end

  defp finish_deadline({:error, :timeout}, _budget), do: :ok
  defp finish_deadline(_resolution, budget), do: DeadlineBudget.ensure_active(budget)
end
