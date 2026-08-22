defmodule FerricStore.HTTP.Client do
  @moduledoc false

  use GenServer

  alias FerricStore.{AsyncDelivery, AsyncRequest, ClientIdentity, ClientShutdown}
  alias FerricStore.HTTP.{Command, Options}
  alias FerricStore.SDK.Native.AdmissionGate

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
         [{:submission_admission, gate}] <- safe_lookup(endpoint, :submission_admission),
         :ok <- AdmissionGate.acquire(gate) do
      try do
        Command.execute(config, message, timeout)
      after
        AdmissionGate.release(gate)
      end
    else
      {:error, :dead} -> {:error, :client_closed}
      {:error, :unknown} -> {:error, {:client_unavailable, :unknown}}
      [] -> {:error, :client_closed}
      {:error, _reason} = error -> error
    end
  end

  @spec submit_async(pid(), term(), timeout()) :: :ok | {:error, term()}
  def submit_async(client, message, timeout),
    do: GenServer.call(client, {:submit_async, message, timeout}, timeout)

  @spec cancel(pid(), pid(), reference(), timeout()) :: :ok | {:error, term()}
  def cancel(client, owner, ref, timeout),
    do: GenServer.call(client, {:cancel_async, owner, ref}, timeout)

  @spec control(pid(), term(), timeout()) :: {:error, term()}
  def control(_client, request, _timeout),
    do: {:error, {:http_native_only, control_name(request)}}

  @impl true
  def init(config) do
    endpoint = :ets.new(__MODULE__, [:set, :protected, read_concurrency: true])
    gate = AdmissionGate.new(config.max_concurrent_requests)
    ClientIdentity.mark(:http, endpoint)

    true =
      :ets.insert(endpoint, [{:client, self()}, {:config, config}, {:submission_admission, gate}])

    {:ok, %{tasks: %{}}}
  end

  @impl true
  def handle_call({:submit_async, message, timeout}, _from, state) do
    case async_identity(message) do
      {:ok, owner, ref, synchronous} ->
        client = self()
        {worker, monitor} = spawn_monitor(fn -> run_async(client, ref, synchronous, timeout) end)
        tasks = Map.put(state.tasks, {owner, ref}, {worker, monitor})
        {:reply, :ok, %{state | tasks: tasks}}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:cancel_async, owner, ref}, _from, state) do
    case Map.pop(state.tasks, {owner, ref}) do
      {{worker, monitor}, tasks} ->
        Process.demonitor(monitor, [:flush])
        Process.exit(worker, :kill)
        {:reply, :ok, %{state | tasks: tasks}}

      {nil, _tasks} ->
        {:reply, :ok, state}
    end
  end

  @impl true
  def handle_info({:DOWN, monitor, :process, _worker, _reason}, state) do
    tasks =
      Map.reject(state.tasks, fn {_key, {_worker, task_monitor}} -> task_monitor == monitor end)

    {:noreply, %{state | tasks: tasks}}
  end

  @impl true
  def terminate(_reason, state) do
    Enum.each(state.tasks, fn {_key, {worker, _monitor}} -> Process.exit(worker, :kill) end)
    :ok
  end

  defp run_async(client, ref, message, timeout) do
    AsyncDelivery.deliver(ref, AsyncRequest, submit(client, message, timeout))
  end

  defp async_identity({:async_request, owner, ref, opcode, payload, context}),
    do: {:ok, owner, ref, {:request, opcode, payload, context}}

  defp async_identity({:async_command, owner, ref, opcode, _key, payload, context}),
    do: {:ok, owner, ref, {:request, opcode, payload, context}}

  defp async_identity(_invalid), do: {:error, {:http_unsupported, :async_request_shape}}

  defp safe_lookup(endpoint, key) do
    :ets.lookup(endpoint, key)
  rescue
    ArgumentError -> []
  end

  defp control_name({name, _value}), do: name
  defp control_name(name), do: name
end
