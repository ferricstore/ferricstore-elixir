defmodule FerricStore.SDK.InvocationError do
  @moduledoc false

  def invalid(operation, field, reason, value) do
    {:error,
     {:invalid_invocation_input,
      %{operation: operation, field: field, reason: reason, value: value}}}
  end
end
