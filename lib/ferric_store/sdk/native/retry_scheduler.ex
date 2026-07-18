defmodule FerricStore.SDK.Native.RetryScheduler do
  @moduledoc false

  alias FerricStore.SDK.Native.RetryPolicy

  @spec request(reference(), term()) :: :ready | :waiting
  def request(tag, reason) when is_reference(tag),
    do: schedule({:retry_request, tag}, reason)

  @spec batch(reference(), term()) :: :ready | :waiting
  def batch(batch_id, reason) when is_reference(batch_id),
    do: schedule({:retry_batch, batch_id}, reason)

  defp schedule(message, reason) do
    case RetryPolicy.retry_after_ms(reason) do
      0 ->
        :ready

      delay ->
        Process.send_after(self(), message, delay)
        :waiting
    end
  end
end
