defmodule FerricStore.HTTP.Client do
  @moduledoc false

  use GenServer

  alias FerricStore.{ClientIdentity, ClientShutdown, Timeout}
  alias FerricStore.HTTP.{ClientAdmission, ClientTasks, Command, Options}

  @spec start_link(binary(), keyword()) :: GenServer.on_start()
  def start_link(url, opts) when is_binary(url) and is_list(opts) do
    with {:ok, config} <- Options.new(url, opts),
         do: GenServer.start_link(__MODULE__, config)
  end

  @spec close(pid(), timeout()) :: :ok | {:error, term()}
  def close(client, timeout), do: ClientShutdown.stop(client, timeout)

  @spec config(pid()) :: Options.t() | nil
  def config(client) when is_pid(client) do
    with {:ok, endpoint} <- ClientIdentity.endpoint(client),
         [{:config, config}] <- :ets.lookup(endpoint, :config),
         do: config
  rescue
    ArgumentError -> nil
  end

  @spec submit(pid(), term(), timeout()) :: term()
  def submit(client, message, timeout) do
    with {:ok, endpoint} <- ClientIdentity.endpoint(client),
         [{:config, config}] <- safe_lookup(endpoint, :config),
         {:ok, lease} <- ClientAdmission.reserve(client, timeout) do
      try do
        Command.execute(config, message, timeout)
      after
        GenServer.cast(client, {:release_submission, lease})
      end
    else
      {:error, :dead} -> {:error, :client_closed}
      {:error, :unknown} -> {:error, {:client_unavailable, :unknown}}
      [] -> {:error, :client_closed}
      {:error, _reason} = error -> error
    end
  end

  @spec submit_async(pid(), term(), timeout()) :: :ok | {:error, term()}
  def submit_async(client, message, timeout) do
    GenServer.call(client, {:submit_async, message, timeout}, timeout)
  catch
    :exit, reason -> ClientAdmission.call_error(reason)
  end

  @spec cancel(pid(), pid(), reference(), timeout()) :: :ok | {:error, term()}
  def cancel(client, owner, ref, timeout) do
    if Timeout.valid?(timeout) do
      GenServer.call(client, {:cancel_async, owner, ref}, timeout)
    else
      {:error, {:cancel_failed, {:invalid_timeout, timeout}}}
    end
  catch
    :exit, reason -> {:error, {:cancel_failed, ClientAdmission.call_reason(reason)}}
  end

  @spec control(pid(), term(), timeout()) :: {:error, term()}
  def control(_client, request, _timeout),
    do: {:error, {:http_native_only, control_name(request)}}

  @impl true
  def init(config) do
    endpoint = :ets.new(__MODULE__, [:set, :protected, read_concurrency: true])
    admission = ClientAdmission.new(config.max_concurrent_requests)
    ClientIdentity.mark(:http, endpoint)

    true =
      :ets.insert(endpoint, [
        {:client, self()},
        {:config, config},
        {:submission_admission, admission.gate}
      ])

    {:ok,
     %{
       admission: admission,
       config: config,
       tasks: ClientTasks.new()
     }}
  end

  @impl true
  def handle_call({:reserve_submission, lease}, {owner, _tag}, state) do
    case ClientAdmission.acquire(state.admission, lease, owner) do
      {:ok, admission} -> {:reply, {:ok, lease}, %{state | admission: admission}}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:submit_async, message, timeout}, {caller, _tag}, state) do
    with {:ok, ^caller, ref, synchronous} <- ClientTasks.identity(message),
         false <- ClientTasks.member?(state.tasks, {caller, ref}),
         lease = make_ref(),
         {:ok, admission} <- ClientAdmission.acquire(state.admission, lease, caller) do
      client = self()

      {worker, worker_monitor} =
        spawn_monitor(fn ->
          ClientTasks.run(client, caller, ref, synchronous, state.config, timeout)
        end)

      task = %{worker: worker, worker_monitor: worker_monitor, lease: lease, cancelled?: false}

      state = %{
        state
        | admission: admission,
          tasks: ClientTasks.put(state.tasks, {caller, ref}, task)
      }

      {:reply, :ok, state}
    else
      true ->
        {:reply, {:error, {:http_unsupported, :duplicate_async_reference}}, state}

      {:ok, _other_owner, _ref, _request} ->
        {:reply, {:error, {:http_unsupported, :invalid_async_owner}}, state}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:cancel_async, owner, ref}, _from, state) do
    case ClientTasks.cancel(state.tasks, {owner, ref}) do
      {:ok, tasks} ->
        {:reply, :ok, %{state | tasks: tasks}}

      :error ->
        {:reply, :ok, state}
    end
  end

  @impl true
  def handle_cast({:release_submission, lease}, state),
    do: {:noreply, release_lease(state, lease)}

  @impl true
  def handle_info({:http_async_result, worker, owner, ref, result}, state) do
    key = {owner, ref}

    case ClientTasks.fetch(state.tasks, key) do
      {:ok, %{worker: ^worker} = task} ->
        Process.demonitor(task.worker_monitor, [:flush])
        {_task, tasks} = ClientTasks.delete(state.tasks, key)
        state = %{state | tasks: tasks} |> release_lease(task.lease)
        FerricStore.AsyncDelivery.deliver(ref, FerricStore.AsyncRequest, result)
        {:noreply, state}

      _stale_or_spoofed ->
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, monitor, :process, process, reason}, state) do
    cond do
      task_entry = ClientTasks.by_monitor(state.tasks, monitor) ->
        {key, task} = task_entry
        {_task, tasks} = ClientTasks.delete(state.tasks, key)
        state = %{state | tasks: tasks} |> release_lease(task.lease)
        ClientTasks.deliver_worker_error(key, task, process, reason)
        {:noreply, state}

      lease = ClientAdmission.owner_lease(state.admission, monitor) ->
        if ClientTasks.for_lease?(state.tasks, lease) do
          {:noreply, %{state | tasks: ClientTasks.cancel_lease(state.tasks, lease)}}
        else
          {:noreply, release_lease(state, lease, false)}
        end

      true ->
        {:noreply, state}
    end
  end

  @impl true
  def terminate(_reason, state) do
    ClientTasks.stop_all(state.tasks)
  end

  defp safe_lookup(endpoint, key) do
    :ets.lookup(endpoint, key)
  rescue
    ArgumentError -> []
  end

  defp release_lease(state, lease, demonitor? \\ true) do
    admission = ClientAdmission.release(state.admission, lease, demonitor?)
    %{state | admission: admission}
  end

  defp control_name({name, _value}), do: name
  defp control_name(name), do: name
end
