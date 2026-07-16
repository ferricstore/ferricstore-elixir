defmodule FerricStore.Transport.ServerFrameAssembler do
  @moduledoc false

  import Bitwise

  alias FerricStore.BinaryDetacher
  alias FerricStore.Transport.{FrameLimits, ServerChunk}

  @more_chunks_flag FerricStore.Protocol.flag_more_chunks()

  defstruct streams: %{},
            buffered_bytes: 0,
            buffered_frames: 0,
            max_streams: 64,
            max_buffer_bytes: 64 * 1024 * 1024,
            max_buffer_frames: 65_536,
            max_frame_bytes: 64 * 1024 * 1024,
            timeout: 30_000

  @type t :: %__MODULE__{
          streams: map(),
          buffered_bytes: non_neg_integer(),
          buffered_frames: non_neg_integer(),
          max_streams: pos_integer(),
          max_buffer_bytes: pos_integer(),
          max_buffer_frames: pos_integer(),
          max_frame_bytes: pos_integer(),
          timeout: timeout()
        }

  @spec new(keyword()) :: t()
  def new(opts) do
    %__MODULE__{
      max_streams: Keyword.fetch!(opts, :max_streams),
      max_buffer_bytes: Keyword.fetch!(opts, :max_buffer_bytes),
      max_buffer_frames:
        Keyword.get(opts, :max_buffer_frames, FrameLimits.max_response_chunk_frames()),
      max_frame_bytes: Keyword.fetch!(opts, :max_frame_bytes),
      timeout: Keyword.fetch!(opts, :timeout)
    }
  end

  @spec append(t(), term(), non_neg_integer(), binary()) ::
          {:ok, t()}
          | {:error,
             :too_many_server_chunk_streams
             | :too_many_server_chunk_frames
             | :server_frame_too_large
             | :server_chunks_too_large}
  def append(%__MODULE__{} = assembler, key, flags, body) do
    {chunk, new_stream?} =
      case Map.fetch(assembler.streams, key) do
        {:ok, chunk} -> {chunk, false}
        :error -> {ServerChunk.new(key, assembler.timeout), true}
      end

    bytes = chunk.bytes + byte_size(body)
    total_bytes = assembler.buffered_bytes + byte_size(body)
    total_frames = assembler.buffered_frames + 1

    cond do
      new_stream? and map_size(assembler.streams) >= assembler.max_streams ->
        ServerChunk.cancel_timer(chunk)
        {:error, :too_many_server_chunk_streams}

      bytes > assembler.max_frame_bytes ->
        ServerChunk.cancel_timer(chunk)
        {:error, :server_frame_too_large}

      total_bytes > assembler.max_buffer_bytes ->
        ServerChunk.cancel_timer(chunk)
        {:error, :server_chunks_too_large}

      total_frames > assembler.max_buffer_frames ->
        ServerChunk.cancel_timer(chunk)
        {:error, :too_many_server_chunk_frames}

      true ->
        body = BinaryDetacher.detach(body)

        chunk = %{
          chunk
          | chunks: [body | chunk.chunks],
            bytes: bytes,
            frames: chunk.frames + 1,
            flags: bor(chunk.flags, flags)
        }

        {:ok,
         %{
           assembler
           | streams: Map.put(assembler.streams, key, chunk),
             buffered_bytes: total_bytes,
             buffered_frames: total_frames
         }}
    end
  end

  @spec complete(t(), term(), non_neg_integer(), binary()) ::
          {:ok, non_neg_integer(), binary(), t()}
          | {:error,
             :server_frame_too_large
             | :server_chunks_too_large
             | :too_many_server_chunk_frames, t()}
  def complete(%__MODULE__{} = assembler, key, flags, body) do
    case complete_parts(assembler, key, flags, body) do
      {:ok, logical_flags, logical_body, assembler} ->
        {:ok, logical_flags, IO.iodata_to_binary(logical_body), assembler}

      {:error, reason, assembler} ->
        {:error, reason, assembler}
    end
  end

  @spec complete_parts(t(), term(), non_neg_integer(), binary()) ::
          {:ok, non_neg_integer(), iodata(), t()}
          | {:error,
             :server_frame_too_large
             | :server_chunks_too_large
             | :too_many_server_chunk_frames, t()}
  def complete_parts(%__MODULE__{} = assembler, key, flags, body) do
    total_bytes = assembler.buffered_bytes + byte_size(body)
    total_frames = assembler.buffered_frames + 1
    {chunk, streams} = Map.pop(assembler.streams, key)
    ServerChunk.cancel_timer(chunk)

    assembler = %{
      assembler
      | streams: streams,
        buffered_bytes: max(assembler.buffered_bytes - ServerChunk.size(chunk), 0),
        buffered_frames: max(assembler.buffered_frames - ServerChunk.frames(chunk), 0)
    }

    cond do
      ServerChunk.size(chunk) + byte_size(body) > assembler.max_frame_bytes ->
        {:error, :server_frame_too_large, assembler}

      total_bytes > assembler.max_buffer_bytes ->
        {:error, :server_chunks_too_large, assembler}

      total_frames > assembler.max_buffer_frames ->
        {:error, :too_many_server_chunk_frames, assembler}

      true ->
        {logical_flags, logical_body} = assemble(chunk, flags, body)
        {:ok, logical_flags, logical_body, assembler}
    end
  end

  @spec timeout?(t(), term(), reference()) :: boolean()
  def timeout?(%__MODULE__{} = assembler, key, token) do
    match?(%{timeout_token: ^token}, Map.get(assembler.streams, key))
  end

  @spec stream_count(t()) :: non_neg_integer()
  def stream_count(%__MODULE__{} = assembler), do: map_size(assembler.streams)

  @spec buffered_bytes(t()) :: non_neg_integer()
  def buffered_bytes(%__MODULE__{} = assembler), do: assembler.buffered_bytes

  @spec buffered_frames(t()) :: non_neg_integer()
  def buffered_frames(%__MODULE__{} = assembler), do: assembler.buffered_frames

  @spec cancel_timers(t()) :: :ok
  def cancel_timers(%__MODULE__{} = assembler) do
    Enum.each(assembler.streams, fn {_key, chunk} -> ServerChunk.cancel_timer(chunk) end)
  end

  defp assemble(nil, flags, body), do: {flags, body}

  defp assemble(chunk, flags, body) do
    logical_flags = bor(chunk.flags, flags) |> band(bnot(@more_chunks_flag))
    logical_body = Enum.reverse([body | chunk.chunks])
    {logical_flags, logical_body}
  end
end
