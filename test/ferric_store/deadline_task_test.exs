defmodule FerricStore.DeadlineTaskTest do
  use ExUnit.Case, async: true

  alias FerricStore.{DeadlineBudget, DeadlineTask}

  test "returns a guarded task result while the deadline is active" do
    assert {:ok, :done} = DeadlineTask.run(DeadlineBudget.new(1_000), fn -> :done end)
  end

  test "does not start work for an expired deadline" do
    test_pid = self()

    assert {:error, :timeout} =
             DeadlineTask.run(DeadlineBudget.new(0), fn -> send(test_pid, :started) end)

    refute_received :started
  end

  test "stops guarded work after a timeout" do
    test_pid = self()

    assert {:error, :timeout} =
             DeadlineTask.run(DeadlineBudget.new(5), fn ->
               send(test_pid, {:worker, self()})
               Process.sleep(:infinity)
             end)

    assert_receive {:worker, worker}
    monitor = Process.monitor(worker)
    assert_receive {:DOWN, ^monitor, :process, ^worker, _reason}, 1_000
  end

  test "contains task throws and exits" do
    assert {:error, {:deadline_task_failed, {:throw, :failed}}} =
             DeadlineTask.run(DeadlineBudget.new(1_000), fn -> throw(:failed) end)

    assert {:error, {:deadline_task_failed, {:exit, :failed}}} =
             DeadlineTask.run(DeadlineBudget.new(1_000), fn -> exit(:failed) end)
  end

  test "an infinite deadline contains an untrappable task exit" do
    owner = self()

    {caller, monitor} =
      spawn_monitor(fn ->
        result =
          DeadlineTask.run(DeadlineBudget.new(:infinity), fn ->
            Process.exit(self(), :kill)
          end)

        send(owner, {:deadline_task_result, result})
      end)

    assert_receive {:deadline_task_result, {:error, {:deadline_task_failed, :killed}}}, 250
    assert_receive {:DOWN, ^monitor, :process, ^caller, :normal}, 250
  end

  test "stops guarded work when its caller exits" do
    test_pid = self()

    caller =
      spawn(fn ->
        DeadlineTask.run(DeadlineBudget.new(5_000), fn ->
          send(test_pid, {:owned_worker, self()})
          Process.sleep(:infinity)
        end)
      end)

    assert_receive {:owned_worker, worker}
    worker_monitor = Process.monitor(worker)
    Process.exit(caller, :kill)

    assert_receive {:DOWN, ^worker_monitor, :process, ^worker, :killed}, 1_000
  end
end
