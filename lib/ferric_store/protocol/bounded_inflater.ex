defmodule FerricStore.Protocol.BoundedInflater do
  @moduledoc false

  @spec inflate(binary(), pos_integer()) ::
          {:ok, binary()}
          | {:error, :decompressed_response_too_large | :invalid_compressed_payload}
  def inflate(body, max_decompressed_bytes)
      when is_binary(body) and is_integer(max_decompressed_bytes) and
             max_decompressed_bytes > 0 do
    stream = :zlib.open()

    try do
      :ok = :zlib.inflateInit(stream, 15, :error)

      stream
      |> :zlib.safeInflate(body)
      |> inflate_with_limit(stream, max_decompressed_bytes, 0, [])
    rescue
      _error -> {:error, :invalid_compressed_payload}
    catch
      _kind, _reason -> {:error, :invalid_compressed_payload}
    after
      :zlib.close(stream)
    end
  end

  defp inflate_with_limit({status, chunk}, stream, limit, total_bytes, chunks)
       when status in [:continue, :finished] do
    chunk_bytes = IO.iodata_length(chunk)
    next_total = total_bytes + chunk_bytes

    cond do
      next_total > limit ->
        {:error, :decompressed_response_too_large}

      status == :finished ->
        :ok = :zlib.inflateEnd(stream)
        {:ok, [chunk | chunks] |> Enum.reverse() |> IO.iodata_to_binary()}

      true ->
        stream
        |> :zlib.safeInflate([])
        |> inflate_with_limit(stream, limit, next_total, [chunk | chunks])
    end
  end

  defp inflate_with_limit(_result, _stream, _limit, _total_bytes, _chunks),
    do: {:error, :invalid_compressed_payload}
end
