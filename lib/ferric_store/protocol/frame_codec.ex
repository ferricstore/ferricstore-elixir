defmodule FerricStore.Protocol.FrameCodec do
  @moduledoc false

  alias FerricStore.Protocol.{BoundedInflater, IodataSizer, PreparedMap}
  alias FerricStore.Protocol.{RequestTooLargeError, ResponseDecoder, ValueCodec}

  @magic "FSNP"
  @request_version 0x01
  @response_version 0x81
  @header_size 24
  @flag_custom_payload 0x02
  @flag_compressed 0x08
  @flag_more_chunks 0x20
  @default_max_decompressed_bytes 64 * 1024 * 1024

  @type frame :: %{
          flags: non_neg_integer(),
          lane_id: non_neg_integer(),
          opcode: non_neg_integer(),
          request_id: non_neg_integer(),
          body_length: non_neg_integer()
        }

  def magic, do: @magic
  def request_version, do: @request_version
  def response_version, do: @response_version
  def header_size, do: @header_size
  def status_ok, do: 0
  def flag_custom_payload, do: @flag_custom_payload
  def flag_compressed, do: @flag_compressed
  def flag_more_chunks, do: @flag_more_chunks

  def encode_request(opcode, request_id, payload, opts) do
    opcode
    |> encode_request_iodata(request_id, payload, opts)
    |> IO.iodata_to_binary()
  end

  def encode_request_iodata(opcode, request_id, payload, opts) do
    flags = Keyword.get(opts, :flags, 0)
    lane_id = Keyword.get(opts, :lane_id, 1)
    max_body_bytes = Keyword.get(opts, :max_body_bytes, :infinity)
    {body, flags, body_length} = encode_request_body(payload, flags, max_body_bytes)

    validate_unsigned!(flags, 8, :flags)
    validate_unsigned!(lane_id, 32, :lane_id)
    validate_unsigned!(opcode, 16, :opcode)
    validate_unsigned!(request_id, 64, :request_id)
    validate_unsigned!(body_length, 32, :body_length)
    validate_body_size!(body_length, max_body_bytes)

    [
      <<@magic::binary, @request_version::8, flags::8, lane_id::32, opcode::16, request_id::64,
        body_length::32>>,
      body
    ]
  end

  def decode_response_header(
        <<@magic::binary, @response_version::8, flags::8, lane_id::32, opcode::16, request_id::64,
          body_length::32>>
      ) do
    {:ok,
     %{
       flags: flags,
       lane_id: lane_id,
       opcode: opcode,
       request_id: request_id,
       body_length: body_length
     }}
  end

  def decode_response_header(_header), do: {:error, :invalid_header}

  def decode_response_body(flags, opcode, body) when is_binary(body) do
    with {:ok, body} <- maybe_decompress(flags, body) do
      ResponseDecoder.decode(opcode, body)
    end
  end

  defp maybe_decompress(flags, body) do
    if Bitwise.band(flags, @flag_compressed) != 0,
      do: BoundedInflater.inflate(body, @default_max_decompressed_bytes),
      else: {:ok, body}
  end

  defp encode_request_body({:custom_payload, body}, flags, max_body_bytes)
       when is_binary(body) or is_list(body) do
    body_length = request_iodata_length(body, max_body_bytes)
    {body, Bitwise.bor(flags, @flag_custom_payload), body_length}
  end

  defp encode_request_body(%PreparedMap{} = payload, flags, max_body_bytes) do
    validate_body_size!(payload.byte_size, max_body_bytes)
    {PreparedMap.to_iodata(payload), flags, payload.byte_size}
  end

  defp encode_request_body(payload, flags, max_body_bytes)
       when is_integer(max_body_bytes) and max_body_bytes > 0 do
    case ValueCodec.encode_iodata(payload, max_body_bytes) do
      {:ok, body, body_length} -> {body, flags, body_length}
      {:error, :too_large} -> raise_too_large!(max_body_bytes)
    end
  end

  defp encode_request_body(payload, flags, _max_body_bytes) do
    body = ValueCodec.encode_iodata(payload)
    {body, flags, IO.iodata_length(body)}
  end

  defp request_iodata_length(body, max_body_bytes)
       when is_integer(max_body_bytes) and max_body_bytes > 0 do
    case IodataSizer.bounded_length(body, max_body_bytes) do
      {:ok, body_length} -> body_length
      {:error, :too_large} -> raise_too_large!(max_body_bytes)
    end
  end

  defp request_iodata_length(body, _max_body_bytes), do: IO.iodata_length(body)

  defp validate_unsigned!(value, bits, field) do
    limit = :erlang.bsl(1, bits)

    unless is_integer(value) and value >= 0 and value < limit,
      do:
        raise(
          ArgumentError,
          "#{field} must be an unsigned #{bits}-bit integer, got: #{inspect(value)}"
        )
  end

  defp validate_body_size!(_body_length, :infinity), do: :ok

  defp validate_body_size!(body_length, limit)
       when is_integer(limit) and limit > 0 and body_length <= limit,
       do: :ok

  defp validate_body_size!(body_length, limit) when is_integer(limit) and limit > 0,
    do: raise(RequestTooLargeError, size: body_length, limit: limit)

  defp validate_body_size!(_body_length, _invalid_limit), do: :ok

  defp raise_too_large!(limit),
    do: raise(RequestTooLargeError, size: limit + 1, limit: limit)
end
