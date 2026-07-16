defmodule FerricStore.SDK.Native.DeadlinePreprocessingTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol.Opcodes
  alias FerricStore.SDK.Native.Client

  @item_limit 100_000

  test "native batch admission starts after the request deadline is created" do
    keys = Enum.map(1..@item_limit, &"native-key-#{&1}")

    assert_budgeted(fn ->
      Client.request(
        self(),
        Opcodes.mget(),
        %{"keys" => keys},
        timeout: 0,
        call_timeout: 0
      )
    end)
  end

  test "pipeline counting starts after the request deadline is created" do
    commands = List.duplicate(["PING"], @item_limit)

    assert_budgeted(fn ->
      Client.pipeline(self(), commands, [], timeout: 0, call_timeout: 0)
    end)
  end

  test "routed item counting starts after the request deadline is created" do
    items = Enum.map(1..@item_limit, &"item-key-#{&1}")

    assert_budgeted(fn ->
      Client.request_by_items(
        self(),
        Opcodes.mget(),
        items,
        & &1,
        &%{"keys" => &1},
        timeout: 0,
        call_timeout: 0
      )
    end)
  end

  defp assert_budgeted(request) do
    # Exclude one-time module loading from the request-path reduction budget.
    assert {:error, :timeout} = request.()

    :erlang.garbage_collect(self())
    {:reductions, before_reductions} = Process.info(self(), :reductions)

    assert {:error, :timeout} = request.()

    {:reductions, after_reductions} = Process.info(self(), :reductions)
    assert after_reductions - before_reductions < 20_000
  end
end
