defmodule FerricStore.SDK.Native.TopologyReplacementDrain do
  @moduledoc false

  alias FerricStore.DeadlineBudget
  alias FerricStore.SDK.Native.ConnectionLifecycle

  @spec await(pid(), map()) :: :ok | {:error, :timeout}
  def await(connection, state) when is_pid(connection) do
    monitor = Process.monitor(connection)
    ConnectionLifecycle.drain(connection)

    receive do
      {:DOWN, ^monitor, :process, ^connection, _reason} -> :ok
    after
      DeadlineBudget.remaining(state.deadline) ->
        Process.demonitor(monitor, [:flush])
        {:error, :timeout}
    end
  end
end
