defmodule FerricStore.Flow.PolicyNormalizer do
  @moduledoc false

  alias FerricStore.DeadlineBudget
  alias FerricStore.Flow.{PolicyStateTraversal, PolicyValueNormalizer}

  def normalize(map) do
    Map.new(map, fn {key, value} -> {key, PolicyValueNormalizer.normalize(key, value)} end)
  end

  @spec normalize(map(), DeadlineBudget.t()) :: {:ok, map()} | {:error, :timeout}
  def normalize(map, %DeadlineBudget{} = budget) when is_map(map) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         {:ok, normalized} <- normalize_options(map, budget),
         :ok <- DeadlineBudget.ensure_active(budget),
         do: {:ok, normalized}
  end

  defp normalize_options(map, budget) do
    Enum.reduce_while(map, {:ok, %{}}, fn
      {"states", states}, {:ok, normalized} ->
        case normalize_states(states, budget) do
          {:ok, states} -> {:cont, {:ok, Map.put(normalized, "states", states)}}
          {:error, _reason} = error -> {:halt, error}
        end

      {key, value}, {:ok, normalized} ->
        value = PolicyValueNormalizer.normalize(key, value)
        {:cont, {:ok, Map.put(normalized, key, value)}}
    end)
  end

  defp normalize_states(states, budget) do
    PolicyStateTraversal.reduce(states, %{}, budget, fn {state, policy}, normalized ->
      state = if is_atom(state), do: Atom.to_string(state), else: state
      policy = PolicyValueNormalizer.normalize(nil, policy)
      {:ok, Map.put(normalized, state, policy)}
    end)
  end
end
