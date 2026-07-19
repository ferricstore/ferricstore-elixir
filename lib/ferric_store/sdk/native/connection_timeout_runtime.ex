defmodule FerricStore.SDK.Native.ConnectionTimeoutRuntime do
  @moduledoc false

  alias FerricStore.SDK.Native.{ConnectionDiscardedResponse, ConnectionDrain, ConnectionRequest}

  @spec handle(non_neg_integer(), reference(), map()) ::
          {:noreply, map()} | {:stop, term(), map()}
  def handle(request_id, token, state) do
    case Map.fetch(state.pending, request_id) do
      {:ok, %{timeout_token: ^token, target: :heartbeat}} ->
        {:stop, :heartbeat_timeout,
         ConnectionRequest.fail_pending(state, {:transport_failed, :heartbeat_timeout})}

      {:ok, %{timeout_token: ^token, phase: :awaiting_delivery}} ->
        {:noreply, state}

      {:ok, %{timeout_token: ^token} = pending} ->
        state = ConnectionDiscardedResponse.timeout(state, request_id, pending)
        {:noreply, ConnectionDrain.maybe_stop(state)}

      _missing_or_stale ->
        {:noreply, state}
    end
  end
end
