defmodule FerricStore.SDK.Native.ConnectionEncodingTaskTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.ConnectionEncodingTask

  test "an active encoding child terminates with its owning encoder" do
    test_pid = self()

    runner =
      spawn(fn ->
        owner_monitor = Process.monitor(test_pid)

        ConnectionEncodingTask.run(
          test_pid,
          owner_monitor,
          %{deadline: :infinity, timeout: :infinity},
          fn _job ->
            send(test_pid, {:encoding_child, self()})
            Process.sleep(:infinity)
          end
        )
      end)

    assert_receive {:encoding_child, child}, 1_000
    child_monitor = Process.monitor(child)

    Process.exit(runner, :kill)

    assert_receive {:DOWN, ^child_monitor, :process, ^child, :killed}, 1_000
  end

  test "an uncatchable encoding child exit does not kill its owning encoder" do
    test_pid = self()

    {runner, runner_monitor} =
      spawn_monitor(fn ->
        owner_monitor = Process.monitor(test_pid)

        result =
          ConnectionEncodingTask.run(
            test_pid,
            owner_monitor,
            %{deadline: :infinity, timeout: :infinity},
            fn _job -> exit(:uncatchable_encoding_failure) end
          )

        send(test_pid, {:encoding_result, result})
        receive do: (:stop -> :ok)
      end)

    assert_receive {:encoding_result, {:error, {:encode_failed, reason}}}, 1_000
    assert reason =~ "uncatchable_encoding_failure"
    refute_receive {:DOWN, ^runner_monitor, :process, ^runner, _reason}, 50

    send(runner, :stop)
    assert_receive {:DOWN, ^runner_monitor, :process, ^runner, :normal}, 1_000
  end
end
