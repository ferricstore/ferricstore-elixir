defmodule FerricStore.Flow.PolicyStateValidator do
  @moduledoc false

  alias FerricStore.DeadlineBudget
  alias FerricStore.Flow.{PolicyRetryValidator, PolicyStateTraversal, PolicyValidation}

  @state_modes [:parallel, :fifo, "parallel", "fifo", "PARALLEL", "FIFO"]

  def validate(nil), do: :ok

  def validate(states) do
    Enum.reduce_while(state_pairs(states), :ok, fn {state, policy}, :ok ->
      state = if is_atom(state), do: Atom.to_string(state), else: state
      result = validate_policy(state, policy)
      if result == :ok, do: {:cont, :ok}, else: {:halt, result}
    end)
  end

  def validate(nil, %DeadlineBudget{} = budget), do: DeadlineBudget.ensure_active(budget)

  def validate(states, %DeadlineBudget{} = budget) when is_map(states) or is_list(states) do
    case PolicyStateTraversal.reduce(states, nil, budget, &validate_state_entry/2) do
      {:ok, nil} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp validate_state_entry({state, policy}, accumulator) do
    state = if is_atom(state), do: Atom.to_string(state), else: state

    case validate_policy(state, policy) do
      :ok -> {:ok, accumulator}
      {:error, _reason} = error -> error
    end
  end

  defp validate_policy(state, policy)
       when is_binary(state) and state != "" and state != "running" do
    policy = PolicyValidation.option_map(policy)

    with :ok <-
           PolicyValidation.allowed(
             Map.fetch(policy, "mode"),
             @state_modes,
             "states.#{state}.mode"
           ),
         :ok <- PolicyRetryValidator.validate(Map.get(policy, "retry"), "states.#{state}.retry") do
      PolicyRetryValidator.validate_retention(
        Map.get(policy, "retention"),
        "states.#{state}.retention"
      )
    end
  end

  defp validate_policy(_state, _policy), do: PolicyValidation.error("states")

  defp state_pairs(states) when is_map(states), do: Map.to_list(states)
  defp state_pairs(states) when is_list(states), do: states
end
