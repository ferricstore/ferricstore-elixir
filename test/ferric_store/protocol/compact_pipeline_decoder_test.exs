defmodule FerricStore.Protocol.CompactPipelineDecoderTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol.CompactPipelineDecoder

  test "ambiguous compact claim layouts are decoded without exponential backtracking" do
    group_count = 14
    claim = compact_base_claim("i", "l", 1)
    body = :binary.copy(claim <> <<0, 0, 0, 0>>, group_count)
    payload = <<group_count * 3::32, body::binary>>
    :erlang.garbage_collect(self())
    {:reductions, before_reductions} = Process.info(self(), :reductions)

    result = CompactPipelineDecoder.decode(payload)

    {:reductions, after_reductions} = Process.info(self(), :reductions)
    assert {:ok, items} = result
    assert length(items) == group_count * 3
    assert Enum.take(items, 3) == [["ok", ["i", nil, "l", 1]], ["ok", nil], ["ok", nil]]
    assert after_reductions - before_reductions < 200_000
  end

  test "a request decode plan handles large valid ambiguous pipelines in linear work" do
    group_count = 1_000
    claim = compact_base_claim("i", "l", 1)
    body = :binary.copy(claim <> <<0, 0, 0, 0>>, group_count)
    payload = <<group_count * 3::32, body::binary>>
    plan = List.flatten(List.duplicate([:base, :unknown, :unknown], group_count))
    :erlang.garbage_collect(self())
    {:reductions, before_reductions} = Process.info(self(), :reductions)

    result = CompactPipelineDecoder.decode(payload, plan)

    {:reductions, after_reductions} = Process.info(self(), :reductions)
    assert {:ok, items} = result
    assert length(items) == group_count * 3
    assert after_reductions - before_reductions < 300_000
  end

  test "a request decode plan must match the server result count" do
    payload = <<2::32, 0, 0, 0, 0>>

    assert {:error, :compact_pipeline_plan_mismatch} =
             CompactPipelineDecoder.decode(payload, [:unknown])

    assert {:error, :compact_pipeline_plan_mismatch} =
             CompactPipelineDecoder.decode(payload, [:unknown, :unknown, :unknown])
  end

  test "an improper request decode plan returns an error instead of raising" do
    payload = <<1::32, 0, 0>>

    assert {:error, :invalid_compact_pipeline_plan} =
             CompactPipelineDecoder.decode(payload, [:unknown | :invalid_tail])

    assert {:error, :invalid_compact_pipeline_plan} =
             CompactPipelineDecoder.decode(payload, [:invalid_mode])
  end

  defp compact_base_claim(id, lease, fencing_token) do
    IO.iodata_to_binary([
      <<0, 4, byte_size(id)::32>>,
      id,
      <<0xFFFF_FFFF::32, byte_size(lease)::32>>,
      lease,
      <<fencing_token::signed-64>>
    ])
  end
end
