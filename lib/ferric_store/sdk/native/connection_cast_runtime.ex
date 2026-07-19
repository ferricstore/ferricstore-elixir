defmodule FerricStore.SDK.Native.ConnectionCastRuntime do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    ConnectionCancellation,
    ConnectionDrain,
    ConnectionInfoRuntime,
    ConnectionPending,
    ConnectionRequest,
    ConnectionTermination
  }

  @spec handle(term(), map()) :: {:noreply, map()} | {:stop, term(), map()}
  def handle(
        {:async_request, delivery, reply_to, tag, opcode, payload, lane_id, timeout, deadline},
        state
      ) do
    target = {delivery, reply_to, tag}

    case ConnectionRequest.submit(state, target, opcode, payload, lane_id, timeout, deadline) do
      {:ok, next_state} ->
        {:noreply, next_state}

      {:error, reason, next_state} ->
        ConnectionPending.reply(target, {:error, reason})
        {:noreply, next_state}
    end
  end

  def handle({:cancel, reply_to, tag}, state) do
    state = ConnectionCancellation.cancel_async_target(state, reply_to, tag)
    {:noreply, ConnectionDrain.maybe_stop(state)}
  end

  def handle({:acknowledge_response, reply_to, tag, delivery_token}, state) do
    message = {:ferricstore_response_delivered, reply_to, tag, delivery_token}
    ConnectionInfoRuntime.handle(message, state)
  end

  def handle(:drain, state), do: {:noreply, ConnectionDrain.begin(state)}

  def handle({:abort, reason}, state) do
    {:stop, :normal, ConnectionRequest.fail_pending(state, reason)}
    |> ConnectionTermination.handle()
  end
end
