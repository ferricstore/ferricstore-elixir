defmodule FerricStore.SDK.Native.ConnectionCancellation do
  @moduledoc false

  alias FerricStore.SDK.Native.ConnectionPending

  @spec cancel_async_target(map(), pid(), reference()) :: map()
  def cancel_async_target(state, reply_to, tag) do
    state
    |> ConnectionPending.cancel_target({:message, reply_to, tag})
    |> ConnectionPending.cancel_target({:acknowledged_message, reply_to, tag})
  end
end
