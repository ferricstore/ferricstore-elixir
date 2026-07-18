defmodule FerricStore.SDK.Native.SessionBootstrap do
  @moduledoc false

  alias FerricStore.Protocol.Opcodes
  alias FerricStore.SDK.Native.{Connection, ServerContract, Topology}

  @type request_timeout :: (-> {:ok, timeout()} | {:error, term()})

  @spec establish(pid(), keyword()) :: {:ok, Topology.t() | nil} | {:error, term()}
  def establish(conn, opts) when is_pid(conn) and is_list(opts) do
    request_timeout = Keyword.fetch!(opts, :request_timeout)

    with {:ok, timeout} <- request_timeout.(),
         {:ok, hello} <- hello(conn, Keyword.fetch!(opts, :client_name), timeout),
         :ok <- ServerContract.validate(hello),
         :ok <- require_credentials(hello, Keyword.get(opts, :password)),
         {:ok, timeout} <- request_timeout.(),
         :ok <-
           authenticate(
             conn,
             Keyword.get(opts, :username),
             Keyword.get(opts, :password),
             timeout
           ),
         :ok <- subscribe_events(conn, Keyword.get(opts, :events, []), request_timeout),
         {:ok, topology} <-
           maybe_load_topology(conn, Keyword.get(opts, :topology_endpoint), request_timeout),
         {:ok, timeout} <- request_timeout.(),
         :ok <- Connection.complete_bootstrap(conn, hello, timeout),
         {:ok, _remaining} <- request_timeout.() do
      {:ok, topology}
    end
  end

  defp hello(conn, client_name, timeout) do
    payload = %{
      "client_name" => client_name,
      "compression" => "none"
    }

    Connection.request(conn, Opcodes.hello(), payload, 0, timeout)
  end

  defp authenticate(_conn, nil, nil, _timeout), do: :ok

  defp authenticate(conn, nil, password, timeout) when is_binary(password),
    do: authenticate(conn, "default", password, timeout)

  defp authenticate(_conn, username, nil, _timeout) when is_binary(username),
    do: {:error, :missing_password}

  defp authenticate(conn, username, password, timeout) do
    case Connection.request(
           conn,
           Opcodes.auth(),
           %{"username" => username, "password" => password},
           0,
           timeout
         ) do
      {:ok, _value} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp require_credentials(hello, nil) do
    if Map.get(hello, "auth_required", Map.get(hello, :auth_required)),
      do: {:error, :missing_password},
      else: :ok
  end

  defp require_credentials(_startup, _password), do: :ok

  defp subscribe_events(_conn, [], _request_timeout), do: :ok

  defp subscribe_events(conn, events, request_timeout) when is_list(events) do
    with {:ok, timeout} <- request_timeout.(),
         {:ok, _value} <-
           Connection.request(
             conn,
             Opcodes.subscribe_events(),
             %{"events" => events},
             0,
             timeout
           ) do
      :ok
    end
  end

  def load_topology(conn, endpoint, timeout) do
    with {:ok, shards} <- Connection.request(conn, Opcodes.shards(), %{}, 0, timeout) do
      Topology.build(shards, default_endpoint: endpoint)
    end
  end

  defp maybe_load_topology(_conn, nil, _request_timeout), do: {:ok, nil}

  defp maybe_load_topology(conn, endpoint, request_timeout) when is_map(endpoint) do
    with {:ok, timeout} <- request_timeout.() do
      load_topology(conn, endpoint, timeout)
    end
  end
end
