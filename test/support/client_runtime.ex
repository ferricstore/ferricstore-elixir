defmodule FerricStore.Test.ClientRuntime do
  @moduledoc false

  alias FerricStore.SDK.Native.{AdmissionGate, ClientSupervisor}

  def coordinator(client), do: ClientSupervisor.coordinator!(client)
  def state(client), do: client |> coordinator() |> :sys.get_state()
  def suspend(client), do: client |> coordinator() |> :sys.suspend()
  def resume(client), do: client |> coordinator() |> :sys.resume()

  @doc false
  def release_submission(%AdmissionGate{} = gate), do: AdmissionGate.release(gate)

  def wrap(result, opts \\ [])

  def wrap({:ok, coordinator}, opts) when is_pid(coordinator) and is_list(opts) do
    event_source = Keyword.get(opts, :event_source, coordinator)
    __MODULE__.Endpoint.start_link({coordinator, event_source})
  end

  def wrap({:error, _reason} = error, _opts), do: error

  defmodule Endpoint do
    @moduledoc false

    use GenServer

    alias FerricStore.ClientIdentity
    alias FerricStore.SDK.Native.AdmissionGate

    def start_link(coordinator), do: GenServer.start_link(__MODULE__, coordinator)

    @impl true
    def init({coordinator, event_source}) do
      endpoint = :ets.new(__MODULE__, [:set, :protected, read_concurrency: true])
      ClientIdentity.mark(:topology_aware, endpoint)

      true =
        :ets.insert(endpoint, [
          {:client, self()},
          {:coordinator, coordinator},
          {:event_source, event_source},
          {:submission_admission, AdmissionGate.new(1_024)}
        ])

      {:ok, %{coordinator: coordinator, monitor: Process.monitor(coordinator)}}
    end

    @impl true
    def handle_info(
          {:DOWN, monitor, :process, coordinator, reason},
          %{coordinator: coordinator, monitor: monitor} = state
        ),
        do: {:stop, reason, state}

    def handle_info(_message, state), do: {:noreply, state}
  end
end
