defmodule FerricStore.Protocol.CompactPipelineItems do
  @moduledoc false

  alias FerricStore.Protocol.{CompactClaimDecoder, CompactPipelineItemDecoder, DecodeBudget}

  @claim_modes [:state_attrs, :state, :attrs, :base]
  @max_failed_states 16_384

  @spec decode(non_neg_integer(), binary(), [atom()] | :unknown) ::
          {:ok, [list()]} | {:error, term()}
  def decode(count, bytes, plan \\ :unknown) do
    with {:ok, budget} <- DecodeBudget.consume(DecodeBudget.new(), count) do
      {result, _failed_states} =
        decode_items(count, bytes, [], budget, MapSet.new(), plan)

      result
    end
  end

  defp decode_items(0, <<>>, acc, _budget, failed_states, plan)
       when plan in [[], :unknown],
       do: {{:ok, Enum.reverse(acc)}, failed_states}

  defp decode_items(0, <<>>, _acc, _budget, failed_states, [_mode | _plan]),
    do: {{:error, :compact_pipeline_plan_mismatch}, failed_states}

  defp decode_items(0, <<>>, _acc, _budget, failed_states, _invalid_plan),
    do: {{:error, :invalid_compact_pipeline_plan}, failed_states}

  defp decode_items(0, _rest, _acc, _budget, failed_states, _plan),
    do: {{:error, :trailing_compact_pipeline_bytes}, failed_states}

  defp decode_items(count, bytes, acc, budget, failed_states, plan) do
    key = {count, byte_size(bytes), budget}

    cond do
      MapSet.member?(failed_states, key) ->
        {{:error, :invalid_compact_pipeline_item}, failed_states}

      MapSet.size(failed_states) >= @max_failed_states ->
        {{:error, :compact_pipeline_ambiguity_limit}, failed_states}

      true ->
        case take_plan(plan) do
          {:ok, claim_mode, remaining_plan} ->
            count
            |> decode_uncached(
              bytes,
              acc,
              budget,
              failed_states,
              claim_mode,
              remaining_plan
            )
            |> cache_failure(key)

          {:error, reason} ->
            {{:error, reason}, failed_states}
        end
    end
  end

  defp decode_uncached(count, bytes, acc, budget, failed_states, mode, plan) do
    case CompactPipelineItemDecoder.decode(bytes, budget) do
      {:ok, item, rest, budget} ->
        decode_items(count - 1, rest, [item | acc], budget, failed_states, plan)

      {:claim, rest} ->
        modes = if mode in @claim_modes, do: [mode], else: @claim_modes
        decode_claim(modes, count, rest, acc, budget, failed_states, plan)

      {:error, _reason} = error ->
        {error, failed_states}
    end
  end

  defp decode_claim([mode | modes], count, bytes, acc, budget, failed_states, plan) do
    case CompactClaimDecoder.take_item(bytes, mode, budget) do
      {:ok, row, rest, remaining_budget} ->
        case decode_items(
               count - 1,
               rest,
               [["ok", row] | acc],
               remaining_budget,
               failed_states,
               plan
             ) do
          {{:ok, _items} = ok, failed_states} ->
            {ok, failed_states}

          {{:error, reason} = error, failed_states}
          when reason in [:compact_pipeline_plan_mismatch, :invalid_compact_pipeline_plan] ->
            {error, failed_states}

          {{:error, _reason}, failed_states} ->
            decode_claim(modes, count, bytes, acc, budget, failed_states, plan)
        end

      {:error, _reason} ->
        decode_claim(modes, count, bytes, acc, budget, failed_states, plan)
    end
  end

  defp decode_claim([], _count, _bytes, _acc, _budget, failed_states, _plan),
    do: {{:error, :invalid_compact_pipeline_claim_job}, failed_states}

  defp take_plan([mode | plan]) when mode in [:state_attrs, :state, :attrs, :base, :unknown],
    do: {:ok, mode, plan}

  defp take_plan(:unknown), do: {:ok, :unknown, :unknown}
  defp take_plan([]), do: {:error, :compact_pipeline_plan_mismatch}
  defp take_plan(_invalid), do: {:error, :invalid_compact_pipeline_plan}

  defp cache_failure({{:error, _reason} = error, failed_states}, key),
    do: {error, MapSet.put(failed_states, key)}

  defp cache_failure(success, _key), do: success
end
