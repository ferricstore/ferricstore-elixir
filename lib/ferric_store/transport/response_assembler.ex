defmodule FerricStore.Transport.ResponseAssembler do
  @moduledoc false

  import Bitwise

  alias FerricStore.BinaryDetacher

  @more_chunks_flag FerricStore.Protocol.flag_more_chunks()

  @spec append(
          map(),
          non_neg_integer(),
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          keyword()
        ) ::
          {:ok, map(), non_neg_integer(), non_neg_integer()}
          | {:error,
             :response_too_large
             | :response_buffers_too_large
             | :response_chunk_frames_too_large}
  def append(pending, flags, body, buffered_bytes, buffered_frames, opts) do
    chunk_bytes = pending.chunk_bytes + byte_size(body)
    chunk_frames = pending.chunk_frames + 1
    next_buffered_bytes = buffered_bytes + byte_size(body)
    next_buffered_frames = buffered_frames + 1

    cond do
      chunk_bytes > Keyword.fetch!(opts, :max_response_bytes) ->
        {:error, :response_too_large}

      next_buffered_bytes > Keyword.fetch!(opts, :max_buffer_bytes) ->
        {:error, :response_buffers_too_large}

      chunk_frames > Keyword.fetch!(opts, :max_buffer_frames) or
          next_buffered_frames > Keyword.fetch!(opts, :max_buffer_frames) ->
        {:error, :response_chunk_frames_too_large}

      true ->
        body = BinaryDetacher.detach(body)

        pending = %{
          pending
          | chunks: [body | pending.chunks],
            chunk_bytes: chunk_bytes,
            chunk_frames: chunk_frames,
            flags: bor(pending.flags, flags)
        }

        {:ok, pending, next_buffered_bytes, next_buffered_frames}
    end
  end

  @spec complete(
          map(),
          non_neg_integer(),
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          keyword()
        ) ::
          {:ok, non_neg_integer(), binary()}
          | {:error,
             :response_too_large
             | :response_buffers_too_large
             | :response_chunk_frames_too_large}
  def complete(pending, flags, body, buffered_bytes, buffered_frames, opts) do
    case complete_parts(pending, flags, body, buffered_bytes, buffered_frames, opts) do
      {:ok, logical_flags, logical_body} ->
        {:ok, logical_flags, IO.iodata_to_binary(logical_body)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @spec complete_parts(
          map(),
          non_neg_integer(),
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          keyword()
        ) ::
          {:ok, non_neg_integer(), iodata()}
          | {:error,
             :response_too_large
             | :response_buffers_too_large
             | :response_chunk_frames_too_large}
  def complete_parts(pending, flags, body, buffered_bytes, buffered_frames, opts) do
    chunk_bytes = pending.chunk_bytes + byte_size(body)
    next_buffered_bytes = buffered_bytes + byte_size(body)
    next_buffered_frames = buffered_frames + 1

    cond do
      chunk_bytes > Keyword.fetch!(opts, :max_response_bytes) ->
        {:error, :response_too_large}

      next_buffered_bytes > Keyword.fetch!(opts, :max_buffer_bytes) ->
        {:error, :response_buffers_too_large}

      pending.chunk_frames + 1 > Keyword.fetch!(opts, :max_buffer_frames) or
          next_buffered_frames > Keyword.fetch!(opts, :max_buffer_frames) ->
        {:error, :response_chunk_frames_too_large}

      true ->
        logical_body =
          case pending.chunks do
            [] -> body
            chunks -> Enum.reverse([body | chunks])
          end

        logical_flags = bor(pending.flags, flags) |> band(bnot(@more_chunks_flag))
        {:ok, logical_flags, logical_body}
    end
  end
end
