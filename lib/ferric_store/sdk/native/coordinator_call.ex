defmodule FerricStore.SDK.Native.CoordinatorCall do
  @moduledoc false

  alias FerricStore.ClientIdentity
  alias FerricStore.HTTP.Client, as: HTTPClient
  alias FerricStore.SDK.Native.{AdmissionGate, ClientSupervisor}

  @type error :: {:error, :client_closed | :timeout | {:client_unavailable, term()}}

  @spec call(term(), term(), timeout()) :: term() | error()
  def call(client, message, timeout \\ 5_000)

  def call(client, message, timeout) when is_pid(client) do
    case ClientIdentity.type(client) do
      :http -> HTTPClient.control(client, message, timeout)
      _native -> native_call(client, message, timeout)
    end
  catch
    :exit, reason -> normalize_exit(reason)
  end

  def call(_client, _message, _timeout), do: invalid_client()

  @spec submit(term(), term(), timeout()) :: term() | error()
  def submit(client, message, timeout) when is_pid(client) do
    case ClientIdentity.type(client) do
      :http -> HTTPClient.submit(client, message, timeout)
      _native -> native_submit(client, message, timeout)
    end
  catch
    :exit, reason -> normalize_exit(reason)
  end

  def submit(_client, _message, _timeout), do: invalid_client()

  defp native_submit(client, message, timeout) do
    with {:ok, coordinator} <- resolve(client),
         {:ok, gate} <- resolve_submission_admission(client),
         :ok <- AdmissionGate.acquire(gate) do
      GenServer.call(coordinator, {:admitted_submission, gate, message}, timeout)
    end
  end

  @spec submit_async(term(), term(), timeout()) :: term() | error()
  def submit_async(client, message, timeout) do
    case ClientIdentity.type(client) do
      :http -> HTTPClient.submit_async(client, message, timeout)
      _native -> submit(client, {:async_submission, message}, timeout)
    end
  end

  @spec cast(term(), term()) :: :ok | error()
  def cast(client, message) when is_pid(client) do
    case ClientIdentity.type(client) do
      :http -> HTTPClient.control(client, message, 5_000)
      _native -> native_cast(client, message)
    end
  catch
    :exit, reason -> normalize_exit(reason)
  end

  def cast(_client, _message), do: invalid_client()

  defp native_call(client, message, timeout) do
    with {:ok, coordinator} <- resolve(client),
         do: GenServer.call(coordinator, message, timeout)
  end

  defp native_cast(client, message) do
    with {:ok, coordinator} <- resolve(client),
         do: GenServer.cast(coordinator, message)
  end

  defp normalize_exit({:timeout, {GenServer, :call, _request}}), do: {:error, :timeout}

  defp normalize_exit({reason, {GenServer, :call, _request}})
       when reason in [:noproc, :normal, :shutdown],
       do: {:error, :client_closed}

  defp normalize_exit({{:shutdown, _detail}, {GenServer, :call, _request}}),
    do: {:error, :client_closed}

  defp normalize_exit(reason) when reason in [:noproc, :normal, :shutdown],
    do: {:error, :client_closed}

  defp normalize_exit({:shutdown, _detail}), do: {:error, :client_closed}

  defp normalize_exit({reason, {GenServer, :call, _request}}),
    do: {:error, {:client_unavailable, reason}}

  defp normalize_exit(reason), do: {:error, {:client_unavailable, reason}}

  defp invalid_client, do: {:error, {:client_unavailable, :invalid_client}}

  defp resolve(client) do
    case ClientSupervisor.coordinator(client) do
      {:ok, coordinator} -> {:ok, coordinator}
      {:error, :client_closed} -> {:error, :client_closed}
      {:error, :invalid_client} -> {:error, {:client_unavailable, :invalid_client}}
    end
  end

  defp resolve_submission_admission(client) do
    case ClientSupervisor.submission_admission(client) do
      {:ok, gate} -> {:ok, gate}
      {:error, :client_closed} -> {:error, :client_closed}
      {:error, :invalid_client} -> {:error, {:client_unavailable, :invalid_client}}
    end
  end
end
