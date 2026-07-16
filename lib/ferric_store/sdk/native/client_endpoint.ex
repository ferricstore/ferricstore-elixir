defmodule FerricStore.SDK.Native.ClientEndpoint do
  @moduledoc false

  alias FerricStore.{ClientIdentity, FailureFormatter}
  alias FerricStore.SDK.Native.{AdmissionGate, Topology}

  @type client_error :: :client_closed | :invalid_client
  @type publication_error ::
          client_error() | :endpoint_not_owned | :endpoint_unavailable

  @spec register_client(:ets.tid(), pid()) :: :ok
  def register_client(endpoint, client) when is_pid(client) do
    true = :ets.insert(endpoint, {:client, client})
    :ok
  end

  @spec put_submission_admission(:ets.tid(), AdmissionGate.t()) :: :ok
  def put_submission_admission(endpoint, %AdmissionGate{} = gate) do
    true = :ets.insert(endpoint, {:submission_admission, gate})
    :ok
  end

  @spec put_event_source(:ets.tid(), pid()) :: :ok
  def put_event_source(endpoint, source) when is_pid(source) do
    true = :ets.insert(endpoint, {:event_source, source})
    :ok
  end

  @spec coordinator(pid()) :: {:ok, pid()} | {:error, client_error()}
  def coordinator(client) when is_pid(client) do
    with {:ok, endpoint} <- resolve(client),
         [{:coordinator, coordinator}] <- lookup(endpoint),
         true <- Process.alive?(coordinator) do
      {:ok, coordinator}
    else
      {:error, _reason} = error -> error
      _missing_or_dead -> {:error, :client_closed}
    end
  end

  @spec topology_snapshot(term()) ::
          {:ok, reference(), Topology.t()} | {:error, client_error()}
  def topology_snapshot(client) when is_pid(client) do
    with {:ok, endpoint} <- resolve(client),
         [{:topology, version, %Topology{} = topology}] <- lookup(endpoint, :topology) do
      {:ok, version, topology}
    else
      {:error, _reason} = error -> error
      _missing -> {:error, :client_closed}
    end
  end

  def topology_snapshot(_client), do: {:error, :invalid_client}

  @spec submission_admission(pid()) :: {:ok, AdmissionGate.t()} | {:error, client_error()}
  def submission_admission(client) when is_pid(client) do
    with {:ok, endpoint} <- resolve(client),
         [{:submission_admission, %AdmissionGate{} = gate}] <-
           lookup(endpoint, :submission_admission) do
      {:ok, gate}
    else
      {:error, _reason} = error -> error
      _missing -> {:error, :client_closed}
    end
  end

  @spec event_source(pid()) :: {:ok, pid()} | {:error, client_error()}
  def event_source(client) when is_pid(client) do
    with {:ok, endpoint} <- resolve(client),
         [{:event_source, source}] <- lookup(endpoint, :event_source),
         true <- Process.alive?(source) do
      {:ok, source}
    else
      {:error, _reason} = error -> error
      _missing_or_dead -> {:error, :client_closed}
    end
  end

  @spec publish_topology(pid(), reference(), Topology.t()) ::
          :ok | {:error, publication_error()}
  def publish_topology(client, version, %Topology{} = topology)
      when is_pid(client) and is_reference(version) do
    with {:ok, endpoint} <- resolve(client),
         :ok <- require_owner(endpoint) do
      true = :ets.insert(endpoint, {:topology, version, topology})
      :ok
    end
  rescue
    ArgumentError -> {:error, :endpoint_unavailable}
  end

  @spec hand_off(:ets.tid(), pid()) :: :ok | {:error, term()}
  def hand_off(endpoint, coordinator) when is_pid(coordinator) do
    {version, %Topology{} = topology} = GenServer.call(coordinator, :endpoint_snapshot)

    true =
      :ets.insert(endpoint, [
        {:coordinator, coordinator},
        {:topology, version, topology}
      ])

    true = :ets.give_away(endpoint, coordinator, :client_endpoint)
    :ok
  rescue
    error ->
      {:error, {:error, FailureFormatter.exception_message(error, "endpoint handoff failed")}}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  @spec delete(:ets.tid(), result) :: result when result: term()
  def delete(endpoint, result) do
    :ets.delete(endpoint)
    result
  rescue
    ArgumentError -> result
  end

  defp resolve(client) do
    case ClientIdentity.endpoint(client) do
      {:ok, endpoint} -> {:ok, endpoint}
      {:error, :dead} -> {:error, :client_closed}
      {:error, :unknown} -> {:error, :invalid_client}
    end
  end

  defp lookup(endpoint, key \\ :coordinator) do
    :ets.lookup(endpoint, key)
  rescue
    ArgumentError -> []
  end

  defp require_owner(endpoint) do
    case :ets.info(endpoint, :owner) do
      owner when owner == self() -> :ok
      :undefined -> {:error, :endpoint_unavailable}
      _other -> {:error, :endpoint_not_owned}
    end
  end
end
