defmodule FerricStore.SDK.Native.TopologyBootstrap do
  @moduledoc false

  alias FerricStore.DeadlineBudget

  alias FerricStore.SDK.Native.{
    Connection,
    ConnectionLifecycle,
    EndpointPolicy,
    SessionBootstrap,
    Topology
  }

  @type result :: {:ok, Topology.t(), pid(), term(), map()} | {:error, term()}

  @spec run([map()], keyword()) :: result()
  def run(candidates, opts) when is_list(candidates) and is_list(opts) do
    context = %{
      supervisor: Keyword.fetch!(opts, :connection_supervisor),
      endpoint_policy: Keyword.fetch!(opts, :endpoint_policy),
      endpoint_trust: Keyword.fetch!(opts, :endpoint_trust),
      endpoint_validator: Keyword.get(opts, :endpoint_validator),
      username: Keyword.get(opts, :username),
      password: Keyword.get(opts, :password),
      client_name: Keyword.fetch!(opts, :client_name),
      events: Keyword.get(opts, :events, []),
      deadline: DeadlineBudget.new(Keyword.fetch!(opts, :timeout))
    }

    bootstrap_candidates(
      candidates,
      context,
      {:error, :no_endpoint_reachable},
      length(candidates)
    )
  end

  defp bootstrap_candidates([], context, last_result, 0) do
    if remaining_timeout(context) == 0, do: {:error, :timeout}, else: last_result
  end

  defp bootstrap_candidates([endpoint | rest], context, _last_result, candidate_count) do
    if remaining_timeout(context) == 0 do
      {:error, :timeout}
    else
      candidate_context = %{
        context
        | deadline: DeadlineBudget.slice(context.deadline, candidate_count)
      }

      case bootstrap_candidate(endpoint, candidate_context) do
        {:ok, _topology, _connection, _key, _capacity} = ok ->
          ok

        {:error, _reason} = error ->
          bootstrap_candidates(rest, context, error, candidate_count - 1)
      end
    end
  end

  defp bootstrap_candidate(endpoint, context) do
    with {:ok, validation_timeout} <- request_timeout(context),
         :ok <-
           EndpointPolicy.validate(
             context.endpoint_policy,
             context.endpoint_trust,
             context.endpoint_validator,
             endpoint,
             validation_timeout
           ),
         {:ok, connect_timeout} <- request_timeout(context),
         endpoint = put_connect_timeout(endpoint, connect_timeout),
         {:ok, connection} <- ConnectionLifecycle.start(context.supervisor, endpoint) do
      establish(connection, endpoint, context)
    end
  catch
    :exit, reason -> {:error, reason}
  end

  defp establish(connection, endpoint, context) do
    result =
      with {:ok, topology} <-
             SessionBootstrap.establish(connection,
               client_name: context.client_name,
               username: context.username,
               password: context.password,
               events: context.events,
               topology_endpoint: endpoint,
               request_timeout: fn -> request_timeout(context) end
             ),
           {:ok, timeout} <- request_timeout(context) do
        {:ok, topology, connection, Topology.endpoint_key(endpoint),
         Connection.capacity(connection, timeout)}
      end

    case result do
      {:ok, _topology, _connection, _key, _capacity} = ok ->
        ok

      {:error, _reason} = error ->
        ConnectionLifecycle.stop(context.supervisor, connection)
        error
    end
  catch
    :exit, reason ->
      ConnectionLifecycle.stop(context.supervisor, connection)
      {:error, reason}
  end

  defp remaining_timeout(context), do: DeadlineBudget.remaining(context.deadline)

  defp request_timeout(context), do: DeadlineBudget.request_timeout(context.deadline)

  defp put_connect_timeout(endpoint, :infinity), do: endpoint

  defp put_connect_timeout(endpoint, timeout) do
    configured = Map.get(endpoint, :connect_timeout, timeout)

    connect_timeout =
      if is_integer(configured) and configured >= 0,
        do: min(configured, timeout),
        else: timeout

    Map.put(endpoint, :connect_timeout, connect_timeout)
  end
end
