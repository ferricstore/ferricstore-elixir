defmodule FerricStore.SDK.Native.Coordinator do
  @moduledoc false

  use GenServer

  alias FerricStore.SDK.Native.{CoordinatorInfoRuntime, CoordinatorRuntime}

  @impl true
  def init(opts), do: CoordinatorRuntime.init(opts)

  @impl true
  def handle_call(request, from, state), do: CoordinatorRuntime.call(request, from, state)

  @impl true
  def handle_cast(request, state), do: CoordinatorRuntime.cast(request, state)

  @impl true
  def handle_info(message, state), do: CoordinatorInfoRuntime.handle(message, state)

  @impl true
  def terminate(_reason, state), do: CoordinatorRuntime.terminate(state)
end
