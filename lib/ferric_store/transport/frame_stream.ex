defmodule FerricStore.Transport.FrameStream do
  @moduledoc false

  import Kernel, except: [byte_size: 1]

  alias FerricStore.Protocol

  @header_size 24
  @coalesce_chunk_count 64
  @coalesce_bytes 64 * 1024

  defstruct chunks: {[], []},
            pending_chunks: [],
            pending_count: 0,
            pending_bytes: 0,
            byte_size: 0

  @type t :: %__MODULE__{
          chunks: :queue.queue(binary()),
          pending_chunks: [binary()],
          pending_count: non_neg_integer(),
          pending_bytes: non_neg_integer(),
          byte_size: non_neg_integer()
        }

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec append(t(), binary()) :: t()
  def append(stream, ""), do: stream

  def append(%__MODULE__{} = stream, data) when is_binary(data) do
    stream = %{
      stream
      | pending_chunks: [data | stream.pending_chunks],
        pending_count: stream.pending_count + 1,
        pending_bytes: stream.pending_bytes + :erlang.byte_size(data),
        byte_size: stream.byte_size + :erlang.byte_size(data)
    }

    if stream.pending_count >= @coalesce_chunk_count or
         stream.pending_bytes >= @coalesce_bytes do
      flush_pending(stream)
    else
      stream
    end
  end

  @spec byte_size(t()) :: non_neg_integer()
  def byte_size(%__MODULE__{byte_size: size}), do: size

  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{byte_size: size}), do: size == 0

  @spec next(t(), pos_integer()) ::
          :incomplete | {:ok, Protocol.frame(), binary(), t()} | {:error, term()}
  def next(%__MODULE__{byte_size: size}, _max_frame_bytes) when size < @header_size,
    do: :incomplete

  def next(%__MODULE__{} = stream, max_frame_bytes)
      when is_integer(max_frame_bytes) and max_frame_bytes > 0 do
    stream = flush_pending(stream)
    header_binary = peek(stream, @header_size)

    case Protocol.decode_response_header(header_binary) do
      {:ok, %{body_length: body_length}} when body_length > max_frame_bytes ->
        {:error, :frame_too_large}

      {:ok, header} when stream.byte_size < @header_size + header.body_length ->
        :incomplete

      {:ok, header} ->
        {_header_binary, stream} = take(stream, @header_size)
        {body, stream} = take(stream, header.body_length)
        {:ok, header, body, stream}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp peek(stream, size) do
    {data, _stream} = take(stream, size)
    data
  end

  defp take(stream, 0), do: {"", stream}

  defp take(%__MODULE__{} = stream, size) when size <= stream.byte_size do
    stream = flush_pending(stream)
    {parts, chunks} = take_parts(stream.chunks, size, [])

    data =
      case parts do
        [single] -> single
        parts -> parts |> Enum.reverse() |> IO.iodata_to_binary()
      end

    {data, %{stream | chunks: chunks, byte_size: stream.byte_size - size}}
  end

  defp take_parts(chunks, 0, parts), do: {parts, chunks}

  defp take_parts(chunks, remaining, parts) do
    {{:value, chunk}, chunks} = :queue.out(chunks)
    chunk_size = :erlang.byte_size(chunk)

    cond do
      chunk_size < remaining ->
        take_parts(chunks, remaining - chunk_size, [chunk | parts])

      chunk_size == remaining ->
        {[chunk | parts], chunks}

      true ->
        <<part::binary-size(^remaining), rest::binary>> = chunk
        {[part | parts], :queue.in_r(rest, chunks)}
    end
  end

  defp flush_pending(%__MODULE__{pending_chunks: []} = stream), do: stream

  defp flush_pending(%__MODULE__{} = stream) do
    data =
      case stream.pending_chunks do
        [single] -> single
        chunks -> chunks |> Enum.reverse() |> IO.iodata_to_binary()
      end

    %{
      stream
      | chunks: :queue.in(data, stream.chunks),
        pending_chunks: [],
        pending_count: 0,
        pending_bytes: 0
    }
  end
end
