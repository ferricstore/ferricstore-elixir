defmodule FerricStore.SDK.Native.CoordinatorPendingRequestTimeout do
  @moduledoc false

  alias FerricStore.SDK.Native.{Connection, CoordinatorConnectionRuntime, CoordinatorRuntime}

  @spec handle(map(), reference()) :: {:noreply, map()}
  def handle(state, tag) do
    endpoint_key = CoordinatorConnectionRuntime.pending_endpoint_key(state, tag)

    result =
      case CoordinatorRuntime.pop_pending_request(state, tag) do
        {nil, state} ->
          {:noreply, state}

        {request, state} ->
          cancel_connection(request, tag)

          state =
            state
            |> CoordinatorRuntime.remove_connection_waiter(request[:connection_key], tag)
            |> CoordinatorRuntime.cancel_refresh_waiter({:request_retry, tag})

          CoordinatorRuntime.handle_pending_timeout(state, request)
      end

    CoordinatorRuntime.resume_batch_capacity(result, endpoint_key)
  end

  defp cancel_connection(%{conn: connection}, tag) when is_pid(connection),
    do: Connection.cancel_async(connection, self(), tag)

  defp cancel_connection(_request, _tag), do: :ok
end
