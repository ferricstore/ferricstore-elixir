defmodule FerricStore.SDK.Native.EventTerminal do
  @moduledoc false

  alias FerricStore.DeadlineBudget
  alias FerricStore.SDK.Native.ClientSupervisor

  @spec await(pid(), timeout()) :: {:ok, map()} | {:error, :client_closed} | nil
  def await(client, timeout) do
    with {:ok, source} <- ClientSupervisor.event_source(client) do
      await_source(client, source, DeadlineBudget.new(timeout))
    end
  end

  defp await_source(client, source, deadline) do
    monitor = Process.monitor(source)

    receive do
      {:ferricstore_event, ^client, event} ->
        Process.demonitor(monitor, [:flush])
        {:ok, event}

      {:DOWN, ^monitor, :process, ^source, _reason} ->
        {:error, :client_closed}
    after
      DeadlineBudget.remaining(deadline) ->
        Process.demonitor(monitor, [:flush])
        nil
    end
  end
end
