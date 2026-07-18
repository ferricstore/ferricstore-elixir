defmodule FerricStore.SDK.Native.CoordinatorRetryInfo do
  @moduledoc false

  alias FerricStore.SDK.Native.CoordinatorRuntime

  @spec handle(:retry_request | :retry_batch, reference(), map()) :: {:noreply, map()}
  def handle(:retry_request, tag, state),
    do: CoordinatorRuntime.resume_request_retry(state, tag)

  def handle(:retry_batch, batch_id, state),
    do: {:noreply, CoordinatorRuntime.resume_batch_retry(state, batch_id)}
end
