defmodule FerricStore.SDK.KV.MSetNXCommands do
  @moduledoc false

  alias FerricStore.{DeadlineBudget, RequestContext, RouteKey, RoutingSlot}
  alias FerricStore.Protocol.Opcodes
  alias FerricStore.SDK.KV.{Input, MSetPair}
  alias FerricStore.SDK.Native.KVRequests

  @deadline_check_interval 256

  @spec msetnx(pid(), map() | list(), RequestContext.t()) ::
          {:ok, boolean()} | {:error, term()}
  def msetnx(client, pairs, %RequestContext{} = context) do
    budget = RequestContext.budget(context)

    with {:ok, pairs, item_count} <- Input.mset_pairs(pairs, budget),
         :ok <- require_nonempty(item_count),
         {:ok, route_key, args} <- prepare(pairs, budget),
         result <-
           KVRequests.request_by_key_with_count(
             client,
             Opcodes.command_exec(),
             route_key,
             %{"command" => "MSETNX", "args" => args},
             item_count,
             context
           ) do
      normalize_response(result)
    end
  end

  defp require_nonempty(0), do: {:error, {:invalid_msetnx_pairs, :empty}}
  defp require_nonempty(_item_count), do: :ok

  defp prepare(pairs, budget) when is_map(pairs),
    do: pairs |> Map.to_list() |> prepare(budget)

  defp prepare(pairs, budget) when is_list(pairs),
    do: prepare_pairs(pairs, nil, nil, [], 0, budget)

  defp prepare_pairs([], _slot, first_key, args, _until_check, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         do: {:ok, first_key, Enum.reverse(args)}
  end

  defp prepare_pairs(pairs, slot, first_key, args, 0, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget) do
      prepare_pairs(pairs, slot, first_key, args, @deadline_check_interval, budget)
    end
  end

  defp prepare_pairs([pair | pairs], expected_slot, first_key, args, until_check, budget) do
    with {:ok, {key, value}} <- MSetPair.normalize(pair),
         {:ok, ^key} <- RouteKey.validate(key),
         slot = RoutingSlot.for_key(key),
         :ok <- same_slot(expected_slot, slot) do
      prepare_pairs(
        pairs,
        expected_slot || slot,
        first_key || key,
        [value, key | args],
        until_check - 1,
        budget
      )
    end
  end

  defp same_slot(nil, _slot), do: :ok
  defp same_slot(slot, slot), do: :ok
  defp same_slot(_expected, _actual), do: {:error, {:cross_slot_keys, :msetnx}}

  defp normalize_response({:ok, 1}), do: {:ok, true}
  defp normalize_response({:ok, 0}), do: {:ok, false}

  defp normalize_response({:ok, _value}),
    do: {:error, {:invalid_kv_response, %{operation: :msetnx, reason: :expected_zero_or_one}}}

  defp normalize_response({:error, _reason} = error), do: error
end
