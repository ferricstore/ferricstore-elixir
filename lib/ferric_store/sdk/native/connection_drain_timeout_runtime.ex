defmodule FerricStore.SDK.Native.ConnectionDrainTimeoutRuntime do
  @moduledoc false

  alias FerricStore.SDK.Native.{ConnectionPendingLifecycle, ConnectionRequest}

  @spec handle(reference(), map()) :: {:noreply, map()} | {:stop, :normal, map()}
  def handle(token, %{drain: %{active: true, terminal: true, token: token} = drain} = state) do
    state = %{state | drain: %{drain | timer: nil, token: nil}}
    {:stop, :normal, ConnectionPendingLifecycle.discard_all(state)}
  end

  def handle(token, %{drain: %{active: true, token: token} = drain} = state) do
    state = %{state | drain: %{drain | timer: nil, token: nil}}
    {:stop, :normal, ConnectionRequest.fail_pending(state, :connection_drained)}
  end

  def handle(_token, state), do: {:noreply, state}
end
