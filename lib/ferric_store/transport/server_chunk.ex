defmodule FerricStore.Transport.ServerChunk do
  @moduledoc false

  @spec new(term(), timeout()) :: map()
  def new(key, timeout) do
    token = make_ref()

    %{
      chunks: [],
      bytes: 0,
      frames: 0,
      flags: 0,
      timeout_token: token,
      timer: timer(key, token, timeout)
    }
  end

  @spec size(map() | nil) :: non_neg_integer()
  def size(nil), do: 0
  def size(chunk), do: chunk.bytes

  @spec frames(map() | nil) :: non_neg_integer()
  def frames(nil), do: 0
  def frames(chunk), do: chunk.frames

  @spec cancel_timer(map() | nil) :: :ok
  def cancel_timer(nil), do: :ok
  def cancel_timer(chunk), do: cancel(chunk.timer)

  defp timer(_key, _token, :infinity), do: nil

  defp timer(key, token, timeout) when is_integer(timeout) and timeout >= 0,
    do: Process.send_after(self(), {:server_chunk_timeout, key, token}, timeout)

  defp cancel(nil), do: :ok

  defp cancel(timer) do
    Process.cancel_timer(timer, async: true, info: false)
    :ok
  end
end
