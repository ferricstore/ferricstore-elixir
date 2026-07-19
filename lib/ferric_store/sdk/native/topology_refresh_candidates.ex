defmodule FerricStore.SDK.Native.TopologyRefreshCandidates do
  @moduledoc false

  alias FerricStore.DeadlineBudget

  @spec run([term()], map(), (term(), map() -> term())) :: term()
  def run(candidates, state, refresh) when is_function(refresh, 2) do
    next(candidates, state, refresh, {:error, :no_endpoint_reachable}, length(candidates))
  end

  defp next([], state, _refresh, last_result, 0) do
    if DeadlineBudget.remaining(state.deadline) == 0,
      do: {:error, :timeout},
      else: last_result
  end

  defp next([endpoint | rest], state, refresh, _last_result, candidate_count) do
    if DeadlineBudget.remaining(state.deadline) == 0 do
      {:error, :timeout}
    else
      candidate_state = %{
        state
        | deadline: DeadlineBudget.slice(state.deadline, candidate_count)
      }

      case refresh.(endpoint, candidate_state) do
        {:ok, _topology, _conn, _key, _capacity, _replaced_connection} = ok -> ok
        {:error, _reason} = error -> next(rest, state, refresh, error, candidate_count - 1)
      end
    end
  end
end
