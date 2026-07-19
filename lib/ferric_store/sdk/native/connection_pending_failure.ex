defmodule FerricStore.SDK.Native.ConnectionPendingFailure do
  @moduledoc false

  alias FerricStore.SDK.Native.{ConnectionReply, ConnectionResponseDecoder, ConnectionTimers}

  @spec run(map(), term()) :: map()
  def run(state, reason) do
    {awaiting_delivery, unresolved} =
      Enum.split_with(state.pending, fn {_request_id, pending} ->
        pending[:phase] == :awaiting_delivery
      end)

    Enum.each(unresolved, fn {_request_id, pending} ->
      ConnectionTimers.cancel(pending.timer)
      ConnectionResponseDecoder.stop(pending)
      ConnectionReply.send(pending.target, {:error, reason})
    end)

    pending = Map.new(awaiting_delivery)

    %{
      state
      | pending: pending,
        pending_targets: Map.new(pending, &target_entry/1),
        pending_lanes: lane_counts(pending),
        data_in_flight: flow_controlled_count(pending),
        response_chunk_bytes: pending_sum(pending, :chunk_bytes),
        response_chunk_frames: pending_sum(pending, :chunk_frames)
    }
  end

  defp target_entry({request_id, request}), do: {request.target, request_id}

  defp lane_counts(pending) do
    Enum.reduce(pending, %{}, fn {_request_id, request}, counts ->
      if request.flow_controlled?,
        do: Map.update(counts, request.lane_id, 1, &(&1 + 1)),
        else: counts
    end)
  end

  defp flow_controlled_count(pending) do
    Enum.count(pending, fn {_request_id, request} -> request.flow_controlled? end)
  end

  defp pending_sum(pending, key) do
    Enum.reduce(pending, 0, fn {_request_id, request}, total ->
      total + Map.get(request, key, 0)
    end)
  end
end
