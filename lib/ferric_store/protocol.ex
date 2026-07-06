defmodule FerricStore.Protocol do
  @moduledoc """
  Encoder and decoder for the FerricStore native TCP protocol.

  This module is intentionally small and allocation-conscious because every SDK
  command passes through it. Higher-level modules should build simple command
  argument lists and let this module handle the wire format.
  """

  alias FerricStore.SDK.Native.Opcodes

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
  @op_get 0x0101
  @op_set 0x0102
  @op_mget 0x0104
  @op_mset 0x0105
  @op_pipeline 0x000E
  @op_flow_create 0x0201
  @op_flow_claim_due 0x0203
  @op_flow_complete 0x0204
  @op_flow_value_put 0x020B
  @op_flow_value_mget 0x020C
  @op_flow_create_many 0x020F
  @op_flow_complete_many 0x0210
  @op_flow_policy_set 0x021E
  @op_flow_policy_get 0x021F
  @op_flow_search 0x0230

  @compact_create_many_keys MapSet.new([
                              "items",
                              "type",
                              "state",
                              "now_ms",
                              "run_at_ms",
                              "partition_key",
                              "independent",
                              "return"
                            ])
  @compact_complete_many_keys MapSet.new([
                                "items",
                                "now_ms",
                                "partition_key",
                                "independent",
                                "return"
                              ])

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
  def opcode(:get), do: @op_get
  def opcode(:set), do: @op_set
  def opcode(:mget), do: @op_mget
  def opcode(:mset), do: @op_mset
  def opcode(:pipeline), do: @op_pipeline
  def opcode(:flow_create), do: @op_flow_create
  def opcode(:flow_claim_due), do: @op_flow_claim_due
  def opcode(:flow_complete), do: @op_flow_complete
  def opcode(:flow_value_put), do: @op_flow_value_put
  def opcode(:flow_value_mget), do: @op_flow_value_mget
  def opcode(:flow_create_many), do: @op_flow_create_many
  def opcode(:flow_complete_many), do: @op_flow_complete_many
  def opcode(:flow_policy_set), do: @op_flow_policy_set
  def opcode(:flow_policy_get), do: @op_flow_policy_get
  def opcode(:flow_search), do: @op_flow_search
  def opcode(name) when is_atom(name), do: Opcodes.fetch!(name)

  def custom_payload(payload) when is_binary(payload), do: {:custom_payload, payload}

  def encode_request(opcode, request_id, payload, opts \\ []) do
    flags = Keyword.get(opts, :flags, 0)
    lane_id = Keyword.get(opts, :lane_id, 1)
    {body, flags} = encode_request_body(payload, flags)

    <<@magic::binary, @request_version::8, flags::8, lane_id::32, opcode::16, request_id::64,
      byte_size(body)::32, body::binary>>
  end

  def compact_flow_create_many_payload(
        %{
          "type" => type,
          "state" => state,
          "now_ms" => now_ms,
          "run_at_ms" => run_at_ms,
          "items" => items
        } = payload
      )
      when is_binary(type) and is_binary(state) and is_integer(now_ms) and is_integer(run_at_ms) and
             is_list(items) do
    with :ok <- compact_payload_keys(payload, @compact_create_many_keys),
         {:ok, return_mode} <- compact_create_many_return_mode(Map.get(payload, "return")),
         {:ok, partition_key} <- compact_optional_binary_value(Map.get(payload, "partition_key")),
         {:ok, item_bytes, tag} <- compact_flow_create_many_items(items, partition_key) do
      prefix = [
        <<tag>>,
        compact_binary(type),
        compact_binary(state)
      ]

      partition = if partition_key, do: compact_optional_binary(partition_key), else: []

      {:ok,
       IO.iodata_to_binary([
         prefix,
         partition,
         <<
           now_ms::signed-64,
           run_at_ms::signed-64,
           compact_bool_marker(Map.get(payload, "independent"))::8,
           return_mode::8,
           length(items)::32
         >>,
         item_bytes
       ])}
    end
  end

  def compact_flow_create_many_payload(_payload), do: :error

  def compact_flow_create_many_ids_payload(type, state, partition_key, ids, opts \\ [])
      when is_binary(type) and is_binary(state) and is_list(ids) do
    with {:ok, return_mode} <-
           compact_create_many_return_mode(
             if(Keyword.get(opts, :return_ok_on_success), do: "OK_ON_SUCCESS")
           ),
         {:ok, partition_key} <- compact_optional_binary_value(partition_key),
         {:ok, item_bytes, tag} <- compact_flow_create_many_id_items(ids, partition_key) do
      now_ms = Keyword.get(opts, :now_ms, System.system_time(:millisecond))
      run_at_ms = Keyword.get(opts, :run_at_ms, now_ms)
      partition = if partition_key, do: compact_optional_binary(partition_key), else: []

      {:ok,
       IO.iodata_to_binary([
         <<tag>>,
         compact_binary(type),
         compact_binary(state),
         partition,
         <<
           now_ms::signed-64,
           run_at_ms::signed-64,
           compact_bool_marker(Keyword.get(opts, :independent))::8,
           return_mode::8,
           length(ids)::32
         >>,
         item_bytes
       ])}
    end
  end

  def compact_flow_complete_many_payload(%{"now_ms" => now_ms, "items" => items} = payload)
      when is_integer(now_ms) and is_list(items) do
    with :ok <- compact_payload_keys(payload, @compact_complete_many_keys),
         {:ok, partition_key} <- compact_optional_binary_value(Map.get(payload, "partition_key")),
         {:ok, item_bytes} <- compact_flow_claimed_many_items(items) do
      tag =
        case Map.get(payload, "return") do
          nil -> 0x92
          value when value in ["OK_ON_SUCCESS", "ok_on_success"] -> 0x93
          _other -> :error
        end

      if tag == :error do
        :error
      else
        {:ok,
         IO.iodata_to_binary([
           <<tag>>,
           compact_optional_binary(partition_key),
           <<
             now_ms::signed-64,
             compact_terminal_independent_marker(payload)::8,
             length(items)::32
           >>,
           item_bytes
         ])}
      end
    end
  end

  def compact_flow_complete_many_payload(_payload), do: :error

  defp compact_payload_keys(payload, allowed_keys) do
    if payload |> Map.keys() |> Enum.all?(&MapSet.member?(allowed_keys, &1)) do
      :ok
    else
      :error
    end
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

  def command_payload(command, args \\ [], opts \\ []) do
    %{"command" => normalize_command(command), "args" => Enum.map(args, &normalize_arg/1)}
    |> maybe_put_request_context(opts)
  end

  def pipeline_payload(commands, opts \\ []) when is_list(commands) do
    protocol_commands =
      commands
      |> Enum.with_index(1)
      |> Enum.map(fn {command, request_id} ->
        pipeline_command(command, request_id)
      end)

    payload = %{"atomicity" => "none", "commands" => protocol_commands}

    payload =
      case Keyword.get(opts, :return) do
        :compact -> Map.put(payload, "return", "compact")
        "compact" -> Map.put(payload, "return", "compact")
        :pairs -> Map.put(payload, "return", "pairs")
        "pairs" -> Map.put(payload, "return", "pairs")
        _ -> payload
      end

    maybe_put_request_context(payload, opts)
  end

  defp maybe_put_request_context(payload, opts) do
    case normalize_request_context(Keyword.get(opts, :request_context)) do
      nil -> payload
      context -> Map.put(payload, "request_context", context)
    end
  end

  defp normalize_request_context(nil), do: nil

  defp normalize_request_context(%{} = context) do
    %{}
    |> put_context_value("subject", context_value(context, "subject", :subject))
    |> put_context_value("tenant", context_value(context, "tenant", :tenant))
    |> put_context_scopes(context_value(context, "scopes", :scopes))
    |> empty_to_nil()
  end

  defp normalize_request_context(_context), do: nil

  defp put_context_value(payload, _key, value) when value in [nil, ""], do: payload

  defp put_context_value(payload, key, value) when is_binary(value),
    do: Map.put(payload, key, value)

  defp put_context_value(payload, _key, _value), do: payload

  defp put_context_scopes(payload, scopes) do
    scopes =
      scopes
      |> normalize_scopes()
      |> Enum.uniq()

    case scopes do
      [] -> payload
      scopes -> Map.put(payload, "scopes", scopes)
    end
  end

  defp normalize_scopes(scopes) when is_list(scopes), do: Enum.filter(scopes, &is_binary/1)

  defp normalize_scopes(scopes) when is_binary(scopes) do
    String.split(scopes, [",", " "], trim: true)
  end

  defp normalize_scopes(_scopes), do: []

  defp context_value(%{} = context, string_key, atom_key) do
    Map.get(context, string_key) || Map.get(context, atom_key)
  end

  defp empty_to_nil(map) when map == %{}, do: nil
  defp empty_to_nil(map), do: map

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

  defp encode_request_body({:custom_payload, body}, flags) when is_binary(body),
    do: {body, Bitwise.bor(flags, @flag_custom_payload)}

  defp encode_request_body(payload, flags), do: {encode_value(payload), flags}

  def decode_value(<<0, rest::binary>>), do: {:ok, nil, rest}
  def decode_value(<<1, rest::binary>>), do: {:ok, true, rest}
  def decode_value(<<2, rest::binary>>), do: {:ok, false, rest}
  def decode_value(<<3, value::signed-64, rest::binary>>), do: {:ok, value, rest}

  def decode_value(<<4, length::32, bytes::binary-size(length), rest::binary>>),
    do: {:ok, bytes, rest}

  def decode_value(<<7, value::float-64, rest::binary>>), do: {:ok, value, rest}

  def decode_value(<<5, count::32, rest::binary>>) do
    decode_list(count, rest, [])
  end

  def decode_value(<<6, count::32, rest::binary>>) do
    decode_map(count, rest, %{})
  end

  def decode_value(_), do: {:error, :invalid_value}

  defp decode_uncompressed_response_body(opcode, <<status::16, value_body::binary>>) do
    with {:ok, value} <- decode_response_value(opcode, status, value_body) do
      if status == @status_ok, do: {:ok, value}, else: {:error, {status, value}}
    end
  end

  defp decode_uncompressed_response_body(_opcode, _), do: {:error, :short_response_body}

  defp decode_response_value(opcode, @status_ok, <<0x81, count::32>>)
       when opcode in [@op_set, @op_mset, @op_flow_create_many, @op_flow_complete_many] do
    if count == 1, do: {:ok, "OK"}, else: {:ok, List.duplicate("OK", count)}
  end

  defp decode_response_value(@op_get, @status_ok, <<0x82, 0>>), do: {:ok, nil}

  defp decode_response_value(
         @op_get,
         @status_ok,
         <<0x82, 1, size::32, value::binary-size(size)>>
       ),
       do: {:ok, value}

  defp decode_response_value(opcode, @status_ok, <<0x83, rest::binary>>)
       when opcode in [@op_mget, @op_flow_value_mget, @op_pipeline],
       do: decode_compact_mget(rest)

  defp decode_response_value(opcode, @status_ok, <<0x89, rest::binary>>)
       when opcode in [@op_mget, @op_flow_value_mget, @op_pipeline],
       do: decode_compact_mget_fixed(rest)

  defp decode_response_value(@op_pipeline, @status_ok, <<0x95, rest::binary>>),
    do: decode_compact_pipeline(rest)

  defp decode_response_value(opcode, @status_ok, <<0x80, rest::binary>>)
       when opcode in [@op_flow_claim_due, @op_pipeline],
       do: decode_compact_claim_jobs(rest)

  defp decode_response_value(@op_pipeline, @status_ok, <<0x81, count::32>>),
    do: {:ok, List.duplicate(["ok", "OK"], count)}

  defp decode_response_value(_opcode, _status, value_body) do
    with {:ok, value, rest} <- decode_value(value_body),
         true <- rest == <<>> || {:error, :trailing_response_bytes} do
      {:ok, value}
    end
  end

  defp decode_compact_mget(<<count::32, rest::binary>>),
    do: decode_compact_mget_values(count, rest, [])

  defp decode_compact_mget(_), do: {:error, :invalid_compact_mget}

  defp decode_compact_mget_values(0, <<>>, acc), do: {:ok, Enum.reverse(acc)}
  defp decode_compact_mget_values(0, _rest, _acc), do: {:error, :trailing_compact_mget_bytes}

  defp decode_compact_mget_values(count, <<0, rest::binary>>, acc),
    do: decode_compact_mget_values(count - 1, rest, [nil | acc])

  defp decode_compact_mget_values(
         count,
         <<1, size::32, value::binary-size(size), rest::binary>>,
         acc
       ) do
    decode_compact_mget_values(count - 1, rest, [value | acc])
  end

  defp decode_compact_mget_values(_count, _rest, _acc), do: {:error, :invalid_compact_mget_value}

  defp decode_compact_mget_fixed(<<count::32, size::32, rest::binary>>)
       when byte_size(rest) == count * size do
    values =
      for offset <- 0..max(count - 1, 0), count > 0 do
        binary_part(rest, offset * size, size)
      end

    {:ok, values}
  end

  defp decode_compact_mget_fixed(_), do: {:error, :invalid_compact_mget_fixed}

  defp decode_compact_pipeline(<<count::32, rest::binary>>),
    do: decode_compact_pipeline_items(count, rest, [])

  defp decode_compact_pipeline(_), do: {:error, :invalid_compact_pipeline}

  defp decode_compact_pipeline_items(0, <<>>, acc), do: {:ok, Enum.reverse(acc)}

  defp decode_compact_pipeline_items(0, _rest, _acc),
    do: {:error, :trailing_compact_pipeline_bytes}

  defp decode_compact_pipeline_items(count, <<0, 0, rest::binary>>, acc) do
    decode_compact_pipeline_items(count - 1, rest, [["ok", nil] | acc])
  end

  defp decode_compact_pipeline_items(
         count,
         <<0, 1, size::32, value::binary-size(size), rest::binary>>,
         acc
       ) do
    decode_compact_pipeline_items(count - 1, rest, [["ok", value] | acc])
  end

  defp decode_compact_pipeline_items(
         count,
         <<1, size::32, reason::binary-size(size), rest::binary>>,
         acc
       ) do
    decode_compact_pipeline_items(count - 1, rest, [["busy", reason] | acc])
  end

  defp decode_compact_pipeline_items(
         count,
         <<2, size::32, reason::binary-size(size), rest::binary>>,
         acc
       ) do
    decode_compact_pipeline_items(count - 1, rest, [["error", reason] | acc])
  end

  defp decode_compact_pipeline_items(_count, _rest, _acc),
    do: {:error, :invalid_compact_pipeline_item}

  defp decode_compact_claim_jobs(<<count::32, rest::binary>>) do
    Enum.reduce_while([6, 5, 4], {:error, :invalid_compact_claim_jobs}, fn width, _acc ->
      case decode_compact_claim_job_items(count, rest, [], width) do
        {:ok, items} -> {:halt, {:ok, items}}
        {:error, _reason} = error -> {:cont, error}
      end
    end)
  end

  defp decode_compact_claim_jobs(_), do: {:error, :invalid_compact_claim_jobs}

  defp decode_compact_claim_job_items(0, <<>>, acc, _width), do: {:ok, Enum.reverse(acc)}

  defp decode_compact_claim_job_items(0, _rest, _acc, _width),
    do: {:error, :trailing_compact_claim_job_bytes}

  defp decode_compact_claim_job_items(count, bytes, acc, width) do
    with {:ok, id, rest} <- read_compact_binary(bytes),
         {:ok, partition_key, rest} <- read_compact_optional_binary(rest),
         {:ok, lease_token, <<fencing_token::signed-64, rest::binary>>} <-
           read_compact_binary(rest),
         {:ok, row, rest} <-
           decode_compact_claim_job_tail(
             width,
             [id, partition_key, lease_token, fencing_token],
             rest
           ) do
      decode_compact_claim_job_items(count - 1, rest, [row | acc], width)
    else
      _ -> {:error, :invalid_compact_claim_job}
    end
  end

  defp decode_compact_claim_job_tail(4, row, rest), do: {:ok, row, rest}

  defp decode_compact_claim_job_tail(5, row, rest) do
    case decode_value(rest) do
      {:ok, attrs, rest} when is_map(attrs) -> {:ok, row ++ [attrs], rest}
      _other -> {:error, :invalid_compact_claim_job_attrs}
    end
  end

  defp decode_compact_claim_job_tail(6, row, rest) do
    with {:ok, run_state, rest} <- read_compact_optional_binary(rest),
         {:ok, attrs, rest} when is_map(attrs) <- decode_value(rest) do
      {:ok, row ++ [run_state, attrs], rest}
    else
      _ -> {:error, :invalid_compact_claim_job_state_attrs}
    end
  end

  defp read_compact_binary(<<0xFFFF_FFFF::32, _rest::binary>>),
    do: {:error, :expected_compact_binary}

  defp read_compact_binary(<<size::32, value::binary-size(size), rest::binary>>),
    do: {:ok, value, rest}

  defp read_compact_binary(_), do: {:error, :invalid_compact_binary}

  defp read_compact_optional_binary(<<0xFFFF_FFFF::32, rest::binary>>), do: {:ok, nil, rest}
  defp read_compact_optional_binary(bytes), do: read_compact_binary(bytes)

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

  defp compact_flow_create_many_items(items, nil) do
    cond do
      Enum.all?(items, &(is_list(&1) and length(&1) == 2)) ->
        compact_flow_create_many_regular_items(items, 0x90)

      Enum.all?(items, &(is_list(&1) and length(&1) == 3)) ->
        compact_flow_create_many_mixed_items(items)

      true ->
        :error
    end
  end

  defp compact_flow_create_many_items(items, partition_key) when is_binary(partition_key) do
    if Enum.all?(items, &(is_list(&1) and length(&1) == 2)) do
      compact_flow_create_many_regular_items(items, 0x96)
    else
      :error
    end
  end

  defp compact_flow_create_many_regular_items(items, tag) do
    items
    |> Enum.reduce_while([], fn
      [id, payload], acc when is_binary(id) and is_binary(payload) ->
        {:cont, [[compact_binary(id), compact_binary(payload)] | acc]}

      _item, _acc ->
        {:halt, :error}
    end)
    |> case do
      :error -> :error
      encoded -> {:ok, Enum.reverse(encoded), tag}
    end
  end

  defp compact_flow_create_many_mixed_items(items) do
    items
    |> Enum.reduce_while([], fn
      [id, partition_key, payload], acc
      when is_binary(id) and is_binary(partition_key) and is_binary(payload) ->
        {:cont,
         [[compact_binary(id), compact_binary(partition_key), compact_binary(payload)] | acc]}

      _item, _acc ->
        {:halt, :error}
    end)
    |> case do
      :error -> :error
      encoded -> {:ok, Enum.reverse(encoded), 0x9E}
    end
  end

  defp compact_flow_create_many_id_items(ids, nil) do
    compact_flow_create_many_id_items(ids, 0x90)
  end

  defp compact_flow_create_many_id_items(ids, partition_key) when is_binary(partition_key) do
    compact_flow_create_many_id_items(ids, 0x96)
  end

  defp compact_flow_create_many_id_items(ids, tag) when is_integer(tag) do
    ids
    |> Enum.reduce_while([], fn
      id, acc when is_binary(id) ->
        {:cont, [[compact_binary(id), <<0::32>>] | acc]}

      _id, _acc ->
        {:halt, :error}
    end)
    |> case do
      :error -> :error
      encoded -> {:ok, Enum.reverse(encoded), tag}
    end
  end

  defp compact_flow_claimed_many_items(items) do
    items
    |> Enum.reduce_while([], fn
      [id, lease_token, fencing_token], acc
      when is_binary(id) and is_binary(lease_token) and is_integer(fencing_token) ->
        {:cont,
         [
           [
             compact_binary(id),
             compact_optional_binary(nil),
             compact_binary(lease_token),
             <<fencing_token::signed-64>>
           ]
           | acc
         ]}

      [id, partition_key, lease_token, fencing_token], acc
      when is_binary(id) and is_binary(partition_key) and is_binary(lease_token) and
             is_integer(fencing_token) ->
        {:cont,
         [
           [
             compact_binary(id),
             compact_optional_binary(partition_key),
             compact_binary(lease_token),
             <<fencing_token::signed-64>>
           ]
           | acc
         ]}

      _item, _acc ->
        {:halt, :error}
    end)
    |> case do
      :error -> :error
      encoded -> {:ok, Enum.reverse(encoded)}
    end
  end

  defp compact_binary(value) when is_binary(value), do: [<<byte_size(value)::32>>, value]
  defp compact_optional_binary(nil), do: <<0xFFFF_FFFF::32>>
  defp compact_optional_binary(value) when is_binary(value), do: compact_binary(value)

  defp compact_optional_binary_value(nil), do: {:ok, nil}
  defp compact_optional_binary_value(value) when is_binary(value), do: {:ok, value}
  defp compact_optional_binary_value(_value), do: :error

  defp compact_bool_marker(nil), do: 0
  defp compact_bool_marker(false), do: 1
  defp compact_bool_marker(_true), do: 2

  defp compact_terminal_independent_marker(%{
         "terminal_local_only" => true,
         "independent" => true
       }),
       do: 3

  defp compact_terminal_independent_marker(%{"terminal_local_only" => true}), do: 4

  defp compact_terminal_independent_marker(payload),
    do: compact_bool_marker(Map.get(payload, "independent"))

  defp compact_create_many_return_mode(nil), do: {:ok, 0}
  defp compact_create_many_return_mode("OK_ON_SUCCESS"), do: {:ok, 1}
  defp compact_create_many_return_mode("ok_on_success"), do: {:ok, 1}
  defp compact_create_many_return_mode(_value), do: :error
end
