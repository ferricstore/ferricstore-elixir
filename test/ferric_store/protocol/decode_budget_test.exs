defmodule FerricStore.Protocol.DecodeBudgetTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol

  @nested_item_count 50_000

  test "compact claim rows share one total nested-value budget" do
    attrs = nested_attrs()
    claim = claim_item("flow-1", attrs)

    payload = IO.iodata_to_binary([<<0x80, 2::32>>, claim, claim])

    assert {:error, :collection_too_large} =
             Protocol.decode_compact_response_payload(
               Protocol.opcode(:flow_claim_due),
               payload,
               :attrs
             )
  end

  test "compact pipeline items share one total nested-value budget" do
    nested = nested_list()
    record = <<0x84, 1::32, 1, nested::binary>>
    item = <<0, 2, record::binary>>
    payload = <<0x95, 2::32, item::binary, item::binary>>

    assert {:error, :collection_too_large} =
             Protocol.decode_compact_response_payload(Protocol.opcode(:pipeline), payload)
  end

  defp nested_attrs do
    nested = nested_list()
    <<6, 1::32, 5::32, "items", nested::binary>>
  end

  defp nested_list do
    <<5, @nested_item_count::32, :binary.copy(<<0>>, @nested_item_count)::binary>>
  end

  defp claim_item(id, attrs) do
    <<byte_size(id)::32, id::binary, 0xFFFF_FFFF::32, 5::32, "lease", 1::signed-64,
      attrs::binary>>
  end
end
