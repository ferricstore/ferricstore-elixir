defmodule FerricStore.SDK.Native.BatchGroupPolicy do
  @moduledoc false

  @spec validate([map()], keyword()) :: :ok | {:error, term()}
  def validate(groups, opts) when is_list(groups) and is_list(opts) do
    with :ok <- validate_same_slot(groups, Keyword.get(opts, :require_same_slot)) do
      validate_same_shard(groups, Keyword.get(opts, :require_same_shard))
    end
  end

  defp validate_same_slot(_groups, nil), do: :ok
  defp validate_same_slot([_group], _operation), do: :ok
  defp validate_same_slot([], _operation), do: :ok

  defp validate_same_slot(_groups, operation),
    do: {:error, {:multi_slot_write_requires_explicit_policy, operation}}

  defp validate_same_shard(_groups, nil), do: :ok

  defp validate_same_shard(groups, operation) do
    shards = groups |> Enum.map(& &1.route.shard) |> MapSet.new()

    if MapSet.size(shards) <= 1,
      do: :ok,
      else: {:error, {:multi_shard_write_requires_explicit_policy, operation}}
  end
end
