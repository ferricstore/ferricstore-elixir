defmodule FerricStore.SDK.ManagementInputError do
  @moduledoc false

  def too_many(operation, field, limit, observed),
    do: invalid(operation, field, :too_many_items, %{limit: limit, observed: observed})

  def invalid(operation, field, reason, details) do
    {:error,
     {:invalid_management_input,
      Map.merge(%{operation: operation, field: field, reason: reason}, details)}}
  end
end
