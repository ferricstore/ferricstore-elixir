defmodule FerricStore.SDK.Native.ConnectionStarter do
  @moduledoc false

  use GenServer

  alias FerricStore.DeadlineBudget

  alias FerricStore.SDK.Native.{
    Connection,
    ConnectionLifecycle,
    EndpointValidator,
    SessionBootstrap
  }

  @default_timeout 5_000

  @derive {Inspect, except: [:password]}
  defstruct [
    :owner,
    :token,
    :key,
    :endpoint,
    :connection_supervisor,
    :username,
    :password,
    :client_name,
    :endpoint_validator,
    :deadline,
    timeout: @default_timeout
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  def child_spec(opts) do
    %{
      id: {__MODULE__, make_ref()},
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary,
      type: :worker
    }
  end

  @impl true
  def init(opts) do
    state = struct!(__MODULE__, opts)
    {:ok, %{state | deadline: DeadlineBudget.new(state.timeout)}, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state) do
    result = connect(state)

    send(
      state.owner,
      {:ferricstore_connection_started, self(), state.token, state.key, result}
    )

    {:stop, :normal, state}
  end

  defp connect(state) do
    result =
      with {:ok, timeout} <- request_timeout(state),
           :ok <-
             EndpointValidator.validate(
               state.endpoint_validator,
               state.endpoint,
               timeout
             ) do
        start_connection(state)
      end

    normalize_deadline_error(result, state.deadline)
  catch
    :exit, reason ->
      normalize_deadline_error({:error, {:connect_failed, reason}}, state.deadline)
  end

  defp start_connection(state) do
    endpoint = put_connect_timeout(state.endpoint, DeadlineBudget.remaining(state.deadline))

    case ConnectionLifecycle.start(state.connection_supervisor, endpoint) do
      {:ok, conn} ->
        Process.link(conn)
        establish(conn, state)

      {:error, _reason} = error ->
        error
    end
  end

  defp establish(conn, state) do
    result =
      with {:ok, nil} <-
             SessionBootstrap.establish(conn,
               client_name: state.client_name,
               username: state.username,
               password: state.password,
               request_timeout: fn -> request_timeout(state) end
             ),
           {:ok, timeout} <- request_timeout(state) do
        {:ok, conn, Connection.capacity(conn, timeout)}
      end

    case result do
      {:ok, _conn, _capacity} = ok ->
        ok

      {:error, _reason} = error ->
        ConnectionLifecycle.stop(state.connection_supervisor, conn)
        error
    end
  catch
    :exit, reason ->
      ConnectionLifecycle.stop(state.connection_supervisor, conn)
      {:error, reason}
  end

  defp request_timeout(state), do: DeadlineBudget.request_timeout(state.deadline)

  defp normalize_deadline_error({:error, _reason} = error, deadline) do
    case DeadlineBudget.ensure_active(deadline) do
      :ok -> error
      {:error, :timeout} = timeout -> timeout
    end
  end

  defp normalize_deadline_error(result, _deadline), do: result

  defp put_connect_timeout(endpoint, :infinity), do: endpoint

  defp put_connect_timeout(endpoint, remaining) do
    configured = Map.get(endpoint, :connect_timeout, remaining)

    timeout =
      if is_integer(configured) and configured >= 0,
        do: min(configured, remaining),
        else: remaining

    Map.put(endpoint, :connect_timeout, timeout)
  end
end
