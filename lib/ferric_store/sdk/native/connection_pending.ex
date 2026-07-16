defmodule FerricStore.SDK.Native.ConnectionPending do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    ConnectionDiscardedResponse,
    ConnectionPendingLifecycle,
    ConnectionPendingRegistration
  }

  defdelegate register(state, target, opcode, payload, lane_id, timeout, deadline),
    to: ConnectionPendingRegistration

  defdelegate cancel_target(state, target), to: ConnectionDiscardedResponse, as: :cancel_target
  defdelegate drop(state, request_id, pending), to: ConnectionPendingLifecycle
  defdelegate fail_all(state, reason), to: ConnectionPendingLifecycle
  defdelegate reply(target, result), to: ConnectionPendingLifecycle
end
