defmodule FerricStore.ProtocolResourceBoundaryTest do
  use ExUnit.Case, async: false

  alias FerricStore.Protocol

  test "direct pipeline construction rejects the first command beyond the SDK limit" do
    commands = List.duplicate(["PING"], 100_001)
    :erlang.garbage_collect(self())
    {:reductions, before_reductions} = Process.info(self(), :reductions)

    assert {:error, {:pipeline_too_large, %{items: 100_001, limit: 100_000}}} =
             Protocol.pipeline_payload_result(commands)

    {:reductions, after_reductions} = Process.info(self(), :reductions)
    assert after_reductions - before_reductions < 1_000_000
  end

  test "compact Flow codecs reject over-wide payload maps in constant time" do
    extras = Map.new(1..25_000, &{"unexpected-#{&1}", &1})

    create =
      Map.merge(extras, %{
        "type" => "email",
        "state" => "queued",
        "now_ms" => 0,
        "run_at_ms" => 0,
        "items" => []
      })

    complete = Map.merge(extras, %{"now_ms" => 0, "items" => []})

    assert_fast_error(fn -> Protocol.compact_flow_create_many_payload(create) end)
    assert_fast_error(fn -> Protocol.compact_flow_complete_many_payload(complete) end)
  end

  test "trusted Flow create counts reject excess items before traversing the tail" do
    payload = %{
      "type" => "email",
      "state" => "queued",
      "now_ms" => 0,
      "run_at_ms" => 0,
      "items" => List.duplicate(["flow", ""], 100_000)
    }

    warmup = put_in(payload["items"], [["flow", ""]])
    assert {:ok, _iodata} = Protocol.compact_flow_create_many_iodata_payload(warmup, 1)
    :erlang.garbage_collect(self())
    {:reductions, before_encode} = Process.info(self(), :reductions)
    result = Protocol.compact_flow_create_many_iodata_payload(payload, 1)
    {:reductions, after_encode} = Process.info(self(), :reductions)

    assert :error = result
    assert after_encode - before_encode < 10_000
  end

  defp assert_fast_error(fun) do
    :erlang.garbage_collect(self())
    {:reductions, before_reductions} = Process.info(self(), :reductions)
    assert :error = fun.()
    {:reductions, after_reductions} = Process.info(self(), :reductions)
    assert after_reductions - before_reductions < 50_000
  end
end
