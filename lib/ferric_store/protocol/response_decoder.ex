defmodule FerricStore.Protocol.ResponseDecoder do
  @moduledoc false
  alias FerricStore.BinaryDetacher

  alias FerricStore.Protocol.{
    CommandSpec,
    CompactClaimDecoder,
    CompactMGetDecoder,
    CompactPipelineDecoder,
    CompactValueDecoder,
    ValueCodec
  }

  @status_ok 0
  @max_collection_items FerricStore.RequestLimits.max_batch_items()

  @get_opcode CommandSpec.fetch!(:get).opcode
  @command_exec_opcode CommandSpec.fetch!(:command_exec).opcode
  @claim_opcode CommandSpec.fetch!(:flow_claim_due).opcode
  @pipeline_opcode CommandSpec.fetch!(:pipeline).opcode
  @claim_modes [:base, :attrs, :state, :state_attrs]
  @scalar_ok_opcodes Enum.map([:set, :mset], &CommandSpec.fetch!(&1).opcode)
  @ok_count_opcodes Enum.map(
                      [
                        :flow_create_many,
                        :flow_complete_many,
                        :flow_retry_many,
                        :flow_fail_many,
                        :flow_cancel_many
                      ],
                      &CommandSpec.fetch!(&1).opcode
                    )
  @compact_mget_opcodes Enum.map(
                          [:mget, :flow_value_mget],
                          &CommandSpec.fetch!(&1).opcode
                        )
  @compact_claim_opcodes [@claim_opcode]

  def decode_compact(opcode, payload), do: decode_response_value(opcode, @status_ok, payload)

  def decode_compact(@pipeline_opcode, <<0x95, rest::binary>>, plan) when is_list(plan),
    do: CompactPipelineDecoder.decode(rest, plan)

  def decode_compact(opcode, <<0x80, rest::binary>>, mode)
      when opcode in [@claim_opcode, @command_exec_opcode] and mode in @claim_modes,
      do: CompactClaimDecoder.decode(rest, mode)

  def decode_compact(opcode, payload, _response_context),
    do: decode_response_value(opcode, @status_ok, payload)

  @spec decode(non_neg_integer(), binary()) :: {:ok, term()} | {:error, term()}
  def decode(opcode, <<status::16, value_body::binary>>) do
    with {:ok, value} <- decode_response_value(opcode, status, value_body) do
      if status == @status_ok, do: {:ok, value}, else: {:error, {status, value}}
    end
  end

  def decode(_opcode, _body), do: {:error, :short_response_body}

  defp decode_response_value(opcode, @status_ok, <<0x81, count::32>>)
       when opcode in @scalar_ok_opcodes and count > @max_collection_items,
       do: {:error, :collection_too_large}

  defp decode_response_value(opcode, @status_ok, <<0x81, 1::32>>)
       when opcode in @scalar_ok_opcodes,
       do: {:ok, "OK"}

  defp decode_response_value(opcode, @status_ok, <<0x81, _count::32>>)
       when opcode in @scalar_ok_opcodes,
       do: {:error, :invalid_compact_scalar_count}

  defp decode_response_value(opcode, @status_ok, <<0x81, count::32>>)
       when opcode in @ok_count_opcodes and count <= @max_collection_items do
    if count == 1, do: {:ok, "OK"}, else: {:ok, List.duplicate("OK", count)}
  end

  defp decode_response_value(opcode, @status_ok, <<0x81, _count::32>>)
       when opcode in @ok_count_opcodes,
       do: {:error, :collection_too_large}

  defp decode_response_value(@get_opcode, @status_ok, <<0x82, 0>>), do: {:ok, nil}

  defp decode_response_value(
         @get_opcode,
         @status_ok,
         <<0x82, 1, size::32, value::binary-size(size)>>
       ),
       do: {:ok, BinaryDetacher.detach(value)}

  defp decode_response_value(opcode, @status_ok, <<0x83, rest::binary>>)
       when opcode in @compact_mget_opcodes,
       do: CompactMGetDecoder.decode(rest)

  defp decode_response_value(opcode, @status_ok, <<0x89, rest::binary>>)
       when opcode in @compact_mget_opcodes,
       do: CompactMGetDecoder.decode_fixed(rest)

  defp decode_response_value(_opcode, @status_ok, <<tag, _rest::binary>> = payload)
       when tag in 0x84..0x88,
       do: CompactValueDecoder.decode(payload)

  defp decode_response_value(@pipeline_opcode, @status_ok, <<0x95, rest::binary>>),
    do: CompactPipelineDecoder.decode(rest)

  defp decode_response_value(opcode, @status_ok, <<0x80, rest::binary>>)
       when opcode in @compact_claim_opcodes,
       do: CompactClaimDecoder.decode(rest)

  defp decode_response_value(@pipeline_opcode, @status_ok, <<0x81, count::32>>)
       when count <= @max_collection_items,
       do: {:ok, List.duplicate(["ok", "OK"], count)}

  defp decode_response_value(@pipeline_opcode, @status_ok, <<0x81, _count::32>>),
    do: {:error, :collection_too_large}

  defp decode_response_value(_opcode, _status, value_body) do
    with {:ok, value, rest} <- ValueCodec.decode(value_body),
         true <- rest == <<>> || {:error, :trailing_response_bytes} do
      {:ok, value}
    end
  end
end
