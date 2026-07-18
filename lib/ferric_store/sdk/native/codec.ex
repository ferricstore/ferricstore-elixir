defmodule FerricStore.SDK.Native.Codec do
  @moduledoc false

  import Bitwise

  alias FerricStore.Protocol
  alias FerricStore.Protocol.BoundedInflater

  @flag_custom_payload 0x02
  @flag_compressed 0x08
  @flag_more_chunks 0x20
  @default_max_decompressed_bytes 64 * 1024 * 1024

  @status %{
    0 => :ok,
    1 => :error,
    2 => :auth,
    3 => :noperm,
    4 => :busy,
    5 => :reroute,
    6 => :bad_request
  }

  @spec encode_frame(non_neg_integer(), non_neg_integer(), non_neg_integer(), term()) :: binary()
  def encode_frame(opcode, lane_id, request_id, payload) do
    Protocol.encode_request(opcode, request_id, Protocol.payload_or_empty(payload),
      lane_id: lane_id
    )
  end

  @spec encode_frame(
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          term(),
          pos_integer()
        ) ::
          binary()
  def encode_frame(opcode, lane_id, request_id, payload, max_request_bytes) do
    Protocol.encode_request(opcode, request_id, Protocol.payload_or_empty(payload),
      lane_id: lane_id,
      max_body_bytes: max_request_bytes
    )
  end

  @spec decode_response(non_neg_integer(), non_neg_integer(), binary()) ::
          {:ok, term()} | {:error, term()} | {:auth, term()} | {:noperm, term()} | {:busy, term()}
  def decode_response(opcode, flags, body) do
    decode_response(opcode, flags, body, @default_max_decompressed_bytes)
  end

  @spec decode_response(non_neg_integer(), non_neg_integer(), binary(), pos_integer()) ::
          {:ok, term()} | {:error, term()} | {:auth, term()} | {:noperm, term()} | {:busy, term()}
  def decode_response(opcode, flags, body, max_decompressed_bytes)
      when is_integer(max_decompressed_bytes) and max_decompressed_bytes > 0 do
    decode_response(opcode, flags, body, max_decompressed_bytes, nil)
  end

  @spec decode_response(non_neg_integer(), non_neg_integer(), binary(), pos_integer(), term()) ::
          {:ok, term()} | {:error, term()} | {:auth, term()} | {:noperm, term()} | {:busy, term()}
  def decode_response(opcode, flags, body, max_decompressed_bytes, response_context)
      when is_integer(max_decompressed_bytes) and max_decompressed_bytes > 0 do
    case decode_response_envelope(
           opcode,
           flags,
           body,
           max_decompressed_bytes,
           response_context
         ) do
      {:ok, :ok, value} -> {:ok, value}
      {:ok, :error, value} -> {:error, value}
      {:ok, {:unknown_status, status}, value} -> {:error, {:unknown_status, status, value}}
      {:ok, status, value} -> {status, value}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc false
  @spec decode_response_envelope(non_neg_integer(), non_neg_integer(), binary(), pos_integer()) ::
          {:ok, atom() | {:unknown_status, non_neg_integer()}, term()} | {:error, term()}
  def decode_response_envelope(opcode, flags, body, max_decompressed_bytes)
      when is_integer(max_decompressed_bytes) and max_decompressed_bytes > 0 do
    decode_response_envelope(opcode, flags, body, max_decompressed_bytes, nil)
  end

  defp decode_response_envelope(
         opcode,
         flags,
         body,
         max_decompressed_bytes,
         response_context
       ) do
    with {:ok, body} <- maybe_decompress(flags, body, max_decompressed_bytes),
         {:ok, status, payload} <- split_response_body(body),
         {:ok, value} <- decode_response_payload(opcode, flags, payload, response_context) do
      {:ok, Map.get(@status, status, {:unknown_status, status}), value}
    end
  end

  @spec custom_payload?(non_neg_integer()) :: boolean()
  def custom_payload?(flags), do: band(flags, @flag_custom_payload) != 0

  @spec more_chunks?(non_neg_integer()) :: boolean()
  def more_chunks?(flags), do: band(flags, @flag_more_chunks) != 0

  defp split_response_body(<<status::unsigned-16, payload::binary>>), do: {:ok, status, payload}
  defp split_response_body(_body), do: {:error, :truncated_response}

  defp decode_response_payload(opcode, flags, payload, response_context) do
    if custom_payload?(flags) do
      decode_custom_response_payload(opcode, payload, response_context)
    else
      decode_typed_payload(payload)
    end
  end

  defp decode_custom_response_payload(
         _opcode,
         _payload,
         %{response_plan: _response_plan, compact_codec: nil}
       ),
       do: {:error, :unadvertised_compact_response}

  defp decode_custom_response_payload(
         opcode,
         payload,
         %{response_plan: response_plan, compact_codec: codec}
       )
       when is_binary(codec),
       do: Protocol.decode_compact_response_payload(opcode, payload, response_plan)

  defp decode_custom_response_payload(opcode, payload, response_context),
    do: Protocol.decode_compact_response_payload(opcode, payload, response_context)

  defp decode_typed_payload(payload) do
    case decode_value(payload) do
      {:ok, value, ""} -> {:ok, value}
      {:ok, _value, _rest} -> {:error, :trailing_response_bytes}
      {:error, reason} -> {:error, reason}
    end
  end

  defp maybe_decompress(flags, body, max_decompressed_bytes) do
    if band(flags, @flag_compressed) != 0 do
      BoundedInflater.inflate(body, max_decompressed_bytes)
    else
      {:ok, body}
    end
  end

  defdelegate encode_value(value), to: Protocol
  defdelegate decode_value(value), to: Protocol
end
