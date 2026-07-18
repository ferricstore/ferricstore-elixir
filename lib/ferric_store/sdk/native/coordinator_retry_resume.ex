defmodule FerricStore.SDK.Native.CoordinatorRetryResume do
  @moduledoc false

  alias FerricStore.SDK.Native.RequestRegistry

  def run(state, tag, callbacks) do
    case RequestRegistry.get(state.request_registry, tag) do
      nil -> {:noreply, state}
      _request -> callbacks.start_refresh.(state, {:request_retry, tag})
    end
  end
end
