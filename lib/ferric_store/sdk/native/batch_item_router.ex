defmodule FerricStore.SDK.Native.BatchItemRouter do
  @moduledoc false

  alias FerricStore.FailureFormatter

  @spec call((term() -> term()), term()) :: term()
  def call(item_router, item) do
    item_router.(item)
  rescue
    error ->
      {:error,
       {:route_item_failed, FailureFormatter.exception_message(error, "item routing failed")}}
  catch
    kind, reason -> {:error, {:route_item_failed, {kind, reason}}}
  end
end
