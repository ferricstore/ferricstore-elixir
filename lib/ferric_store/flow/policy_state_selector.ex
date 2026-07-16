defmodule FerricStore.Flow.PolicyStateSelector do
  @moduledoc false

  alias FerricStore.Flow.PolicyValidation

  def validate(options) do
    case Map.fetch(options, "state") do
      :error -> :ok
      {:ok, state} when is_atom(state) -> validate_value(Atom.to_string(state))
      {:ok, state} -> validate_value(state)
    end
  end

  defp validate_value(state)
       when is_binary(state) and state != "" and state != "running",
       do: :ok

  defp validate_value(_state), do: PolicyValidation.error("state")
end
