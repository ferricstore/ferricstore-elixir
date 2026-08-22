defmodule FerricStore.HTTP.ClientTasks do
  @moduledoc false

  alias FerricStore.{AsyncDelivery, AsyncRequest}
  alias FerricStore.HTTP.Command

  @enforce_keys []
  defstruct by_key: %{}, by_lease: %{}, by_monitor: %{}

  @type key :: {pid(), reference()}
  @type task :: %{
          cancelled?: boolean(),
          lease: reference(),
          worker: pid(),
          worker_monitor: reference()
        }
  @type t :: %__MODULE__{
          by_key: %{key() => task()},
          by_lease: %{reference() => key()},
          by_monitor: %{reference() => key()}
        }

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec member?(t(), key()) :: boolean()
  def member?(%__MODULE__{} = tasks, key), do: Map.has_key?(tasks.by_key, key)

  @spec fetch(t(), key()) :: {:ok, task()} | :error
  def fetch(%__MODULE__{} = tasks, key), do: Map.fetch(tasks.by_key, key)

  @spec put(t(), key(), task()) :: t()
  def put(%__MODULE__{} = tasks, key, task) do
    %{
      tasks
      | by_key: Map.put(tasks.by_key, key, task),
        by_lease: Map.put(tasks.by_lease, task.lease, key),
        by_monitor: Map.put(tasks.by_monitor, task.worker_monitor, key)
    }
  end

  @spec delete(t(), key()) :: {task() | nil, t()}
  def delete(%__MODULE__{} = tasks, key) do
    case Map.pop(tasks.by_key, key) do
      {%{lease: lease, worker_monitor: monitor} = task, by_key} ->
        next = %{
          tasks
          | by_key: by_key,
            by_lease: Map.delete(tasks.by_lease, lease),
            by_monitor: Map.delete(tasks.by_monitor, monitor)
        }

        {task, next}

      {nil, _by_key} ->
        {nil, tasks}
    end
  end

  @spec by_monitor(t(), reference()) :: {key(), task()} | nil
  def by_monitor(%__MODULE__{} = tasks, monitor) do
    with {:ok, key} <- Map.fetch(tasks.by_monitor, monitor),
         {:ok, task} <- Map.fetch(tasks.by_key, key) do
      {key, task}
    else
      :error -> nil
    end
  end

  @spec for_lease?(t(), reference()) :: boolean()
  def for_lease?(%__MODULE__{} = tasks, lease), do: Map.has_key?(tasks.by_lease, lease)

  @spec cancel(t(), key()) :: {:ok, t()} | :error
  def cancel(%__MODULE__{} = tasks, key) do
    case Map.fetch(tasks.by_key, key) do
      {:ok, task} ->
        Process.exit(task.worker, :kill)
        {:ok, put_in(tasks.by_key[key].cancelled?, true)}

      :error ->
        :error
    end
  end

  @spec cancel_lease(t(), reference()) :: t()
  def cancel_lease(%__MODULE__{} = tasks, lease) do
    case Map.fetch(tasks.by_lease, lease) do
      {:ok, key} ->
        {:ok, tasks} = cancel(tasks, key)
        tasks

      :error ->
        tasks
    end
  end

  @spec stop_all(t()) :: :ok
  def stop_all(%__MODULE__{} = tasks) do
    Enum.each(tasks.by_key, fn {_key, task} -> Process.exit(task.worker, :kill) end)
    :ok
  end

  @spec identity(term()) :: {:ok, pid(), reference(), term()} | {:error, term()}
  def identity({:async_request, owner, ref, opcode, payload, context}),
    do: {:ok, owner, ref, {:request, opcode, payload, context}}

  def identity({:async_command, owner, ref, opcode, _key, payload, context}),
    do: {:ok, owner, ref, {:request, opcode, payload, context}}

  def identity(_invalid), do: {:error, {:http_unsupported, :async_request_shape}}

  @spec run(pid(), pid(), reference(), term(), struct(), timeout()) :: :ok
  def run(client, owner, ref, message, config, timeout) do
    result = Command.execute(config, message, timeout)
    send(client, {:http_async_result, self(), owner, ref, result})
    :ok
  end

  @spec deliver_worker_error(key(), task(), pid(), term()) :: :ok
  def deliver_worker_error({_owner, ref}, task, process, reason) do
    if not task.cancelled? and task.worker == process do
      AsyncDelivery.deliver(
        ref,
        AsyncRequest,
        {:error, {:client_unavailable, {:http_worker_exit, reason}}}
      )
    else
      :ok
    end
  end
end
