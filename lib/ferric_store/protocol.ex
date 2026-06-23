defmodule FerricStore.Protocol do
  @moduledoc """
  Encoder and decoder for the FerricStore native TCP protocol.

  This module is intentionally small and allocation-conscious because every SDK
  command passes through it. Higher-level modules should build simple command
  argument lists and let this module handle the wire format.
  """

  @magic "FSNP"
  @request_version 0x01
  @response_version 0x81
  @header_size 24

  @flag_custom_payload 0x02
  @flag_compressed 0x08
  @flag_more_chunks 0x20

  @status_ok 0

  @op_startup 0x000C
  @op_auth 0x0002
  @op_ping 0x0003
  @op_command_exec 0x0100
  @op_pipeline 0x000E
  @op_flow_create 0x0201
  @op_flow_complete 0x0204
  @op_flow_value_put 0x020B
  @op_flow_value_mget 0x020C

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
  def status_ok, do: @status_ok
  def flag_custom_payload, do: @flag_custom_payload
  def flag_compressed, do: @flag_compressed
  def flag_more_chunks, do: @flag_more_chunks

  def opcode(:startup), do: @op_startup
  def opcode(:auth), do: @op_auth
  def opcode(:ping), do: @op_ping
  def opcode(:command_exec), do: @op_command_exec
  def opcode(:pipeline), do: @op_pipeline
  def opcode(:flow_create), do: @op_flow_create
  def opcode(:flow_complete), do: @op_flow_complete
  def opcode(:flow_value_put), do: @op_flow_value_put
  def opcode(:flow_value_mget), do: @op_flow_value_mget

  def encode_request(opcode, request_id, payload, opts \\ []) do
    flags = Keyword.get(opts, :flags, 0)
    lane_id = Keyword.get(opts, :lane_id, 1)
    body = encode_value(payload)

    <<@magic::binary, @request_version::8, flags::8, lane_id::32, opcode::16, request_id::64,
      byte_size(body)::32, body::binary>>
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

  def decode_response_header(_), do: {:error, :invalid_header}

  def decode_response_body(flags, opcode, body) when is_binary(body) do
    if Bitwise.band(flags, @flag_compressed) != 0 do
      {:error, :compressed_response_not_supported}
    else
      decode_uncompressed_response_body(opcode, body)
    end
  end

  def command_payload(command, args \\ []) do
    %{"command" => normalize_command(command), "args" => Enum.map(args, &normalize_arg/1)}
  end

  def pipeline_payload(commands) when is_list(commands) do
    protocol_commands =
      commands
      |> Enum.with_index(1)
      |> Enum.map(fn {command, request_id} ->
        pipeline_command(command, request_id)
      end)

    %{"atomicity" => "none", "commands" => protocol_commands}
  end

  defp pipeline_command(%{"opcode" => opcode, "body" => body} = command, request_id)
       when is_integer(opcode) and is_map(body) do
    %{
      "opcode" => opcode,
      "lane_id" => Map.get(command, "lane_id", 1),
      "request_id" => Map.get(command, "request_id", request_id),
      "body" => body
    }
  end

  defp pipeline_command(%{opcode: opcode, body: body} = command, request_id)
       when is_integer(opcode) and is_map(body) do
    %{
      "opcode" => opcode,
      "lane_id" => Map.get(command, :lane_id, 1),
      "request_id" => Map.get(command, :request_id, request_id),
      "body" => body
    }
  end

  defp pipeline_command(command, request_id) do
    [name | args] = List.wrap(command)
    payload = command_payload(name, args)

    %{
      "opcode" => @op_command_exec,
      "lane_id" => 1,
      "request_id" => request_id,
      "body" => payload
    }
  end

  def encode_value(nil), do: <<0>>
  def encode_value(true), do: <<1>>
  def encode_value(false), do: <<2>>
  def encode_value(value) when is_integer(value), do: <<3, value::signed-64>>

  def encode_value(value) when is_binary(value) do
    <<4, byte_size(value)::32, value::binary>>
  end

  def encode_value(value) when is_atom(value) do
    value |> Atom.to_string() |> encode_value()
  end

  def encode_value(value) when is_float(value), do: <<7, value::float-64>>

  def encode_value(value) when is_list(value) do
    encoded = value |> Enum.map(&encode_value/1) |> IO.iodata_to_binary()
    <<5, length(value)::32, encoded::binary>>
  end

  def encode_value(value) when is_tuple(value) do
    value |> Tuple.to_list() |> encode_value()
  end

  def encode_value(value) when is_map(value) do
    entries =
      Enum.map(value, fn {key, item} ->
        key = encode_key(key)
        [<<byte_size(key)::32>>, key, encode_value(item)]
      end)

    <<6, map_size(value)::32, IO.iodata_to_binary(entries)::binary>>
  end

  def decode_value(<<0, rest::binary>>), do: {:ok, nil, rest}
  def decode_value(<<1, rest::binary>>), do: {:ok, true, rest}
  def decode_value(<<2, rest::binary>>), do: {:ok, false, rest}
  def decode_value(<<3, value::signed-64, rest::binary>>), do: {:ok, value, rest}

  def decode_value(<<4, length::32, bytes::binary-size(length), rest::binary>>),
    do: {:ok, bytes, rest}

  def decode_value(<<7, value::float-64, rest::binary>>), do: {:ok, value, rest}

  def decode_value(<<5, count::32, rest::binary>>) do
    with {:ok, values, rest} <- decode_list(count, rest, []) do
      {:ok, values, rest}
    end
  end

  def decode_value(<<6, count::32, rest::binary>>) do
    with {:ok, map, rest} <- decode_map(count, rest, %{}) do
      {:ok, map, rest}
    end
  end

  def decode_value(_), do: {:error, :invalid_value}

  defp decode_uncompressed_response_body(_opcode, <<status::16, value_body::binary>>) do
    with {:ok, value, rest} <- decode_value(value_body),
         true <- rest == <<>> || {:error, :trailing_response_bytes} do
      if status == @status_ok do
        {:ok, value}
      else
        {:error, {status, value}}
      end
    end
  end

  defp decode_uncompressed_response_body(_opcode, _), do: {:error, :short_response_body}

  defp decode_list(0, rest, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_list(count, bytes, acc) do
    with {:ok, value, rest} <- decode_value(bytes) do
      decode_list(count - 1, rest, [value | acc])
    end
  end

  defp decode_map(0, rest, acc), do: {:ok, acc, rest}

  defp decode_map(count, <<key_length::32, key::binary-size(key_length), rest::binary>>, acc) do
    with {:ok, value, rest} <- decode_value(rest) do
      decode_map(count - 1, rest, Map.put(acc, key, value))
    end
  end

  defp decode_map(_count, _bytes, _acc), do: {:error, :invalid_map}

  defp normalize_command(command) when is_atom(command),
    do: command |> Atom.to_string() |> String.upcase()

  defp normalize_command(command) when is_binary(command), do: String.upcase(command)

  defp normalize_arg(value) when is_atom(value), do: Atom.to_string(value)
  defp normalize_arg(value) when is_list(value), do: Enum.map(value, &normalize_arg/1)
  defp normalize_arg(value) when is_tuple(value), do: value |> Tuple.to_list() |> normalize_arg()

  defp normalize_arg(value) when is_map(value) do
    Map.new(value, fn {key, item} -> {encode_key(key), normalize_arg(item)} end)
  end

  defp normalize_arg(value), do: value

  defp encode_key(value) when is_binary(value), do: value
  defp encode_key(value) when is_atom(value), do: Atom.to_string(value)
  defp encode_key(value), do: to_string(value)
end
