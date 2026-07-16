defmodule FerricStore.SDK.Native.ClientSupervisor do
  @moduledoc false

  use Supervisor

  alias FerricStore.ClientIdentity
  alias FerricStore.SDK.Native.{ClientEndpoint, ClientRuntimeStarter, Topology}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) when is_list(opts) do
    owner = self()
    endpoint = :ets.new(__MODULE__, [:set, :protected, read_concurrency: true])

    case Supervisor.start_link(__MODULE__, {:empty, endpoint}) do
      {:ok, supervisor} ->
        :ok = ClientEndpoint.register_client(endpoint, supervisor)

        case ClientRuntimeStarter.start(supervisor, owner, endpoint, opts) do
          {:ok, _supervisor} = started -> started
          {:error, _reason} = error -> ClientEndpoint.delete(endpoint, error)
        end

      {:error, _reason} = error ->
        ClientEndpoint.delete(endpoint, error)
    end
  end

  @impl true
  def init({:empty, endpoint}) do
    ClientIdentity.mark(:topology_aware, endpoint)
    Supervisor.init([], strategy: :one_for_one, auto_shutdown: :any_significant)
  end

  @spec coordinator(pid()) :: {:ok, pid()} | {:error, :client_closed | :invalid_client}
  defdelegate coordinator(client), to: ClientEndpoint

  @doc false
  @spec coordinator!(pid()) :: pid()
  def coordinator!(client) do
    {:ok, coordinator} = coordinator(client)
    coordinator
  end

  @doc false
  @spec topology_snapshot(term()) ::
          {:ok, reference(), Topology.t()} | {:error, :client_closed | :invalid_client}
  defdelegate topology_snapshot(client), to: ClientEndpoint

  @spec submission_admission(pid()) ::
          {:ok, FerricStore.SDK.Native.AdmissionGate.t()}
          | {:error, :client_closed | :invalid_client}
  defdelegate submission_admission(client), to: ClientEndpoint

  @spec event_source(pid()) :: {:ok, pid()} | {:error, :client_closed | :invalid_client}
  defdelegate event_source(client), to: ClientEndpoint
end
