defmodule FerricStore.Flow.ValueRefsValidator do
  @moduledoc false

  alias FerricStore.{DeadlineBudget, RequestLimits, RouteKey}
  alias FerricStore.Flow.Options.CollectionScan

  @max_items RequestLimits.max_batch_items()
  @max_bytes RouteKey.max_bytes()

  @spec validate(term(), DeadlineBudget.t()) :: :ok | {:error, term()}
  def validate(refs, %DeadlineBudget{} = budget) do
    case CollectionScan.validate(refs, @max_items, &valid_ref?/1, budget) do
      {:ok, _count} -> :ok
      {:error, :timeout} = error -> error
      {:error, :too_large} -> batch_too_large()
      {:error, _reason} -> invalid()
    end
  end

  defp valid_ref?(value),
    do: is_binary(value) and value != "" and byte_size(value) <= @max_bytes

  defp invalid,
    do: {:error, {:invalid_flow_value_refs, :expected_nonempty_route_binaries}}

  defp batch_too_large,
    do: {:error, {:batch_too_large, %{items: @max_items + 1, limit: @max_items}}}
end
