defmodule FerricStore.SDK.Native.CoordinatorConnectionStartCompletion do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    ConnectionPool,
    CoordinatorConnectionCleanup,
    CoordinatorRuntime
  }

  @spec handle(map(), pid(), reference(), term(), term()) :: {:noreply, map()}
  def handle(state, starter, token, key, result) do
    case ConnectionPool.pop_attempt(state.connection_pool, key) do
      {%{starter: ^starter, token: ^token} = attempt, pool} ->
        Process.demonitor(attempt.monitor, [:flush])

        state =
          state
          |> Map.put(:connection_pool, pool)
          |> CoordinatorRuntime.delete_lifecycle_monitor(
            attempt.monitor,
            {:connection_attempt, attempt.key}
          )

        CoordinatorRuntime.handle_connection_started(state, attempt, result)

      {attempt, pool} ->
        pool = if attempt, do: ConnectionPool.put_attempt(pool, key, attempt), else: pool
        state = %{state | connection_pool: pool}
        CoordinatorConnectionCleanup.discard_start(state, result)
    end
  end
end
