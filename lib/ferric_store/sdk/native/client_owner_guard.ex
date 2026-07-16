defmodule FerricStore.SDK.Native.ClientOwnerGuard do
  @moduledoc false

  use GenServer

  @spec start_link(pid()) :: GenServer.on_start()
  def start_link(owner) when is_pid(owner), do: GenServer.start_link(__MODULE__, owner)

  @impl true
  def init(owner), do: {:ok, %{owner: owner, monitor: Process.monitor(owner)}}

  @impl true
  def handle_info(
        {:DOWN, monitor, :process, owner, _reason},
        %{owner: owner, monitor: monitor} = state
      ),
      do: {:stop, :normal, state}

  def handle_info(_message, state), do: {:noreply, state}
end
