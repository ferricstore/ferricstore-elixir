defmodule FerricStore.SDK.Native.CoordinatorConnectionResponseRuntime do
  @moduledoc false

  alias FerricStore.SDK.Native.{Connection, ConnectionPool, CoordinatorRuntime}

  @spec handle(map(), tuple()) :: {:noreply, map()}
  def handle(
        state,
        {:ferricstore_connection_response, connection, tag, result, delivery_token}
      )
      when is_pid(connection) and is_reference(tag) and is_reference(delivery_token) do
    Connection.acknowledge_response(connection, self(), tag, delivery_token)
    complete(state, connection, tag, result)
  end

  def handle(state, {:ferricstore_connection_response, connection, tag, result}),
    do: complete(state, connection, tag, result)

  def handle(state, _invalid_response), do: {:noreply, state}

  defp complete(state, connection, tag, result) do
    endpoint_key = ConnectionPool.endpoint_key(state.connection_pool, connection)

    state
    |> CoordinatorRuntime.handle_connection_response(connection, tag, result)
    |> CoordinatorRuntime.resume_batch_capacity(endpoint_key)
  end
end
