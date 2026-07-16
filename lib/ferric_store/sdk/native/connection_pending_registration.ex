defmodule FerricStore.SDK.Native.ConnectionPendingRegistration do
  @moduledoc false

  alias FerricStore.Protocol.CommandSpec
  alias FerricStore.SDK.Native.{ConnectionEncoder, ConnectionTimers, FlowControl}
  alias FerricStore.Transport.SessionPolicy

  @spec register(
          map(),
          term(),
          non_neg_integer(),
          term(),
          non_neg_integer(),
          timeout(),
          integer() | :infinity
        ) :: {:ok, map()}
  def register(state, target, opcode, payload, lane_id, timeout, deadline) do
    request_id = SessionPolicy.available_request_id(state.next_request_id, state.pending)
    timeout_token = make_ref()
    encode_token = make_ref()
    timer = ConnectionTimers.request_timer(request_id, timeout_token, timeout)

    pending = %{
      target: target,
      opcode: opcode,
      lane_id: lane_id,
      flow_controlled?: not CommandSpec.control_lane?(opcode),
      timeout_token: timeout_token,
      encode_token: encode_token,
      timeout: timeout,
      deadline: deadline,
      phase: :encoding,
      response_context: nil,
      timer: timer,
      chunks: [],
      chunk_bytes: 0,
      chunk_frames: 0,
      flags: 0
    }

    next_state =
      state
      |> Map.put(:next_request_id, SessionPolicy.next_request_id(request_id))
      |> Map.put(:pending, Map.put(state.pending, request_id, pending))
      |> Map.put(:pending_targets, Map.put(state.pending_targets, target, request_id))
      |> FlowControl.increment(pending)

    :ok =
      ConnectionEncoder.enqueue(
        next_state,
        request_id,
        encode_token,
        opcode,
        payload,
        lane_id,
        timeout,
        deadline
      )

    {:ok, next_state}
  end
end
