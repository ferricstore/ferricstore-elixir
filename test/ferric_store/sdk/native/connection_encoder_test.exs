defmodule FerricStore.SDK.Native.ConnectionEncoderTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.ConnectionEncoder

  test "stopping an encoder terminates workers that cannot process mailbox messages" do
    control = spawn(fn -> receive do: (:never -> :ok) end)
    data = spawn(fn -> receive do: (:never -> :ok) end)
    workers = [control, data]

    Enum.each(workers, &:erlang.suspend_process/1)

    on_exit(fn ->
      Enum.each(workers, fn worker ->
        if Process.alive?(worker) do
          :erlang.resume_process(worker)
          Process.exit(worker, :kill)
        end
      end)
    end)

    monitors = Enum.map(workers, &Process.monitor/1)
    assert :ok = ConnectionEncoder.stop(%ConnectionEncoder{control: control, data: data})

    Enum.zip(monitors, workers)
    |> Enum.each(fn {monitor, worker} ->
      assert_receive {:DOWN, ^monitor, :process, ^worker, :killed}, 100
    end)
  end
end
