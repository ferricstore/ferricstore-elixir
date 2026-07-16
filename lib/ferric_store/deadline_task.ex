defmodule FerricStore.DeadlineTask do
  @moduledoc false

  alias FerricStore.DeadlineBudget

  @type failure :: {:deadline_task_failed, term()}

  @spec run(DeadlineBudget.t(), (-> result)) ::
          {:ok, result} | {:error, :timeout | failure()}
        when result: term()
  def run(%DeadlineBudget{} = budget, task) when is_function(task, 0) do
    case DeadlineBudget.request_timeout(budget) do
      {:ok, :infinity} -> run_guarded(budget, task, :infinity)
      {:ok, timeout} -> run_guarded(budget, task, timeout)
      {:error, :timeout} = error -> error
    end
  end

  defp run_guarded(budget, task, timeout) do
    owner = self()
    token = make_ref()
    reply_alias = Process.alias()

    {guardian, monitor} =
      spawn_monitor(fn -> guard_task(owner, reply_alias, token, task) end)

    await_task(guardian, monitor, reply_alias, token, budget, timeout)
  end

  defp await_task(guardian, monitor, reply_alias, token, budget, timeout) do
    receive do
      {^token, result} ->
        Process.unalias(reply_alias)
        Process.demonitor(monitor, [:flush])

        case DeadlineBudget.ensure_active(budget) do
          :ok -> result
          {:error, :timeout} = error -> error
        end

      {:DOWN, ^monitor, :process, ^guardian, reason} ->
        Process.unalias(reply_alias)
        {:error, {:deadline_task_failed, reason}}
    after
      timeout ->
        Process.unalias(reply_alias)
        send(guardian, {:cancel_deadline_task, self(), token})
        Process.demonitor(monitor, [:flush])
        {:error, :timeout}
    end
  end

  defp guard_task(owner, reply_alias, token, task) do
    owner_monitor = Process.monitor(owner)
    guardian = self()

    worker =
      spawn_link(fn ->
        send(guardian, {:deadline_task_result, self(), invoke(task)})
      end)

    receive do
      {:deadline_task_result, ^worker, result} ->
        Process.demonitor(owner_monitor, [:flush])
        send(reply_alias, {token, result})

      {:DOWN, ^owner_monitor, :process, ^owner, _reason} ->
        Process.exit(worker, :kill)

      {:cancel_deadline_task, ^owner, ^token} ->
        Process.demonitor(owner_monitor, [:flush])
        Process.exit(worker, :kill)
    end
  end

  defp invoke(task) do
    {:ok, task.()}
  rescue
    error -> {:error, {:deadline_task_failed, {:error, error}}}
  catch
    kind, reason -> {:error, {:deadline_task_failed, {kind, reason}}}
  end
end
