defmodule FerricStore.Flow.PolicyStateStructure do
  @moduledoc false

  alias FerricStore.{BoundedList, DeadlineBudget, RequestLimits}

  alias FerricStore.Flow.{
    PolicyOptionStructure,
    PolicyStateTraversal
  }

  @max_states RequestLimits.max_batch_items()

  def validate(nil), do: :ok

  def validate(states) do
    with {:ok, pairs} <- state_pairs(states), do: validate_state_pairs(pairs)
  end

  def validate(nil, %DeadlineBudget{} = budget),
    do: DeadlineBudget.ensure_active(budget)

  def validate(states, %DeadlineBudget{} = budget) do
    with {:ok, pairs} <- state_pairs(states, budget),
         :ok <- validate_pair_shapes(pairs, budget),
         {:ok, _seen} <-
           PolicyStateTraversal.reduce(pairs, MapSet.new(), budget, &validate_state_pair/2) do
      :ok
    end
  end

  defp validate_state_pairs(pairs) do
    pairs
    |> Enum.reduce_while({:ok, MapSet.new()}, fn pair, {:ok, seen} ->
      case validate_state_pair(pair, seen) do
        {:ok, seen} -> {:cont, {:ok, seen}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> finish_state_pairs()
  end

  defp finish_state_pairs({:ok, _seen}), do: :ok
  defp finish_state_pairs({:error, _reason} = error), do: error

  defp validate_state_pair({state, policy}, seen) do
    with {:ok, state} <- state_name(state),
         :ok <- unique_state(state, seen),
         :ok <- PolicyOptionStructure.validate_state_policy(policy) do
      {:ok, MapSet.put(seen, state)}
    end
  end

  defp state_pairs(states) when is_map(states) and map_size(states) <= @max_states,
    do: {:ok, Map.to_list(states)}

  defp state_pairs(states) when is_map(states), do: states_too_large(map_size(states))

  defp state_pairs(states) when is_list(states) do
    case BoundedList.count(states, @max_states) do
      {:ok, _count} -> valid_state_pairs(states)
      {:error, {:limit_exceeded, observed}} -> states_too_large(observed)
      {:error, :improper_list} -> invalid_states()
    end
  end

  defp state_pairs(_states), do: invalid_states()

  defp state_pairs(states, %DeadlineBudget{} = budget)
       when is_map(states) and map_size(states) <= @max_states,
       do: with(:ok <- DeadlineBudget.ensure_active(budget), do: {:ok, states})

  defp state_pairs(states, %DeadlineBudget{}) when is_map(states),
    do: states_too_large(map_size(states))

  defp state_pairs(states, %DeadlineBudget{} = budget) when is_list(states) do
    case BoundedList.count(states, @max_states, budget) do
      {:ok, _count} -> {:ok, states}
      {:error, {:limit_exceeded, observed}} -> states_too_large(observed)
      {:error, :improper_list} -> invalid_states()
      {:error, :timeout} = error -> error
    end
  end

  defp state_pairs(_states, %DeadlineBudget{} = budget) do
    with :ok <- DeadlineBudget.ensure_active(budget), do: invalid_states()
  end

  defp validate_pair_shapes(states, budget) when is_map(states),
    do: DeadlineBudget.ensure_active(budget)

  defp validate_pair_shapes(states, budget) when is_list(states) do
    case PolicyStateTraversal.reduce(states, nil, budget, fn
           {_state, _policy}, accumulator -> {:ok, accumulator}
           _invalid, _accumulator -> invalid_states()
         end) do
      {:ok, nil} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp valid_state_pairs(states) do
    if Enum.all?(states, &match?({_, _}, &1)), do: {:ok, states}, else: invalid_states()
  end

  defp state_name(state) when is_binary(state) and state != "", do: {:ok, state}
  defp state_name(state) when is_atom(state), do: {:ok, Atom.to_string(state)}
  defp state_name(_state), do: {:error, {:invalid_policy_option, "state"}}

  defp unique_state(state, seen) do
    if MapSet.member?(seen, state),
      do: {:error, {:duplicate_policy_states, [state]}},
      else: :ok
  end

  defp invalid_states, do: {:error, {:invalid_policy_option, "states"}}

  defp states_too_large(observed),
    do: {:error, {:policy_states_too_large, %{limit: @max_states, observed: observed}}}
end
