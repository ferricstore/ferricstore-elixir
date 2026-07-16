defmodule FerricStore.Protocol do
  @moduledoc """
  Encoder and decoder facade for the FerricStore native TCP protocol.
  """

  alias FerricStore.Protocol.{
    CommandPayload,
    FlowBatchCodec,
    FrameCodec,
    Opcodes,
    PipelinePayload,
    ResponseDecoder,
    ValueCodec
  }

  @type frame :: %{
          flags: non_neg_integer(),
          lane_id: non_neg_integer(),
          opcode: non_neg_integer(),
          request_id: non_neg_integer(),
          body_length: non_neg_integer()
        }

  def magic, do: FrameCodec.magic()
  def request_version, do: FrameCodec.request_version()
  def response_version, do: FrameCodec.response_version()
  def header_size, do: FrameCodec.header_size()
  def status_ok, do: FrameCodec.status_ok()
  def flag_custom_payload, do: FrameCodec.flag_custom_payload()
  def flag_compressed, do: FrameCodec.flag_compressed()
  def flag_more_chunks, do: FrameCodec.flag_more_chunks()

  def opcode(name) when is_atom(name), do: Opcodes.fetch!(name)

  def custom_payload(payload) when is_binary(payload) or is_list(payload),
    do: {:custom_payload, payload}

  @doc false
  def payload_or_empty(nil), do: %{}
  def payload_or_empty(payload), do: payload

  def encode_request(opcode, request_id, payload, opts \\ []),
    do: FrameCodec.encode_request(opcode, request_id, payload, opts)

  @doc false
  def encode_request_iodata(opcode, request_id, payload, opts \\ []),
    do: FrameCodec.encode_request_iodata(opcode, request_id, payload, opts)

  def compact_flow_create_many_payload(payload), do: FlowBatchCodec.create_many_payload(payload)

  @doc false
  def compact_flow_create_many_iodata_payload(payload),
    do: FlowBatchCodec.create_many_iodata_payload(payload)

  @doc false
  def compact_flow_create_many_iodata_payload(payload, item_count),
    do: FlowBatchCodec.create_many_iodata_payload(payload, item_count)

  def compact_flow_create_many_ids_payload(type, state, partition_key, ids, opts \\ []),
    do: FlowBatchCodec.create_many_ids_payload(type, state, partition_key, ids, opts)

  @doc false
  def compact_flow_create_many_ids_iodata_payload(type, state, partition_key, ids, opts \\ []),
    do: FlowBatchCodec.create_many_ids_iodata_payload(type, state, partition_key, ids, opts)

  def compact_flow_complete_many_payload(payload),
    do: FlowBatchCodec.complete_many_payload(payload)

  @doc false
  def compact_flow_complete_many_iodata_payload(payload),
    do: FlowBatchCodec.complete_many_iodata_payload(payload)

  @doc false
  def compact_flow_complete_many_iodata_payload(payload, item_count),
    do: FlowBatchCodec.complete_many_iodata_payload(payload, item_count)

  def decode_response_header(header), do: FrameCodec.decode_response_header(header)

  def decode_response_body(flags, opcode, body),
    do: FrameCodec.decode_response_body(flags, opcode, body)

  @doc false
  def decode_compact_response_payload(opcode, payload) when is_binary(payload),
    do: ResponseDecoder.decode_compact(opcode, payload)

  @doc false
  def decode_compact_response_payload(opcode, payload, response_context) when is_binary(payload),
    do: ResponseDecoder.decode_compact(opcode, payload, response_context)

  def command_payload(command, args \\ [], opts \\ []) when is_list(args),
    do: CommandPayload.build(command, args, opts)

  @doc false
  def command_payload_result(command, args \\ [], opts \\ []),
    do: CommandPayload.build_result(command, args, opts)

  def pipeline_payload(commands, opts \\ []) when is_list(commands),
    do: PipelinePayload.build(commands, opts)

  @doc false
  def pipeline_payload_result(commands, opts \\ []),
    do: PipelinePayload.build_result(commands, opts)

  def encode_value(value), do: ValueCodec.encode(value)
  def decode_value(bytes), do: ValueCodec.decode(bytes)
end
