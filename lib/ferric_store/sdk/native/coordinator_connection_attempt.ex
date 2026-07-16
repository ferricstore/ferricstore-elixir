defmodule FerricStore.SDK.Native.CoordinatorConnectionAttempt do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    ConnectionPool,
    ConnectionStarter,
    EndpointPolicy,
    TopologyRuntime
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @spec start(State.t(), term(), map(), term()) ::
          {:waiting, State.t()} | {:error, term(), State.t()}
  def start(state, key, endpoint, waiter) do
    endpoint = TopologyRuntime.endpoint_defaults(endpoint, state)
    token = make_ref()

    options = [
      owner: self(),
      token: token,
      key: key,
      endpoint: endpoint,
      connection_supervisor: state.connection_supervisor,
      username: state.username,
      password: state.password,
      client_name: state.client_name,
      endpoint_validator: state.endpoint_validator,
      timeout: state.topology_refresh_timeout
    ]

    with :ok <-
           EndpointPolicy.validate_policy(state.endpoint_policy, state.endpoint_trust, endpoint),
         {:ok, starter} <- start_worker(state.operation_supervisor, options) do
      attempt = %{
        starter: starter,
        monitor: Process.monitor(starter),
        token: token,
        key: key,
        endpoint: endpoint,
        waiters: MapSet.new([waiter])
      }

      pool = ConnectionPool.put_attempt(state.connection_pool, key, attempt)

      state =
        state
        |> Map.put(:connection_pool, pool)
        |> State.put_lifecycle_monitor(attempt.monitor, {:connection_attempt, key})

      {:waiting, state}
    else
      {:error, reason} -> {:error, normalize_error(reason), state}
    end
  end

  defp start_worker(supervisor, options) do
    DynamicSupervisor.start_child(supervisor, {ConnectionStarter, options})
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp normalize_error({:connect_failed, _reason} = error), do: error
  defp normalize_error(reason), do: {:connect_failed, reason}
end
