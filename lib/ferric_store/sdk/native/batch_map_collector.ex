defmodule FerricStore.SDK.Native.BatchMapCollector do
  @moduledoc false

  alias FerricStore.RequestContext

  @deadline_check_interval 256

  @type collector ::
          (term(), non_neg_integer(), map() -> {:cont, map()} | {:halt, {:error, term()}})

  @spec collect(map(), collector(), RequestContext.t()) :: {:ok, map()} | {:error, term()}
  def collect(items, collector, %RequestContext{} = context) do
    items
    |> Enum.reduce_while({%{}, 0, 0}, fn item, accumulator ->
      collect_item(item, accumulator, collector, context)
    end)
    |> finish()
  end

  defp collect_item(item, {groups, index, 0}, collector, context) do
    case RequestContext.ensure_active(context) do
      :ok -> collect_item(item, {groups, index, @deadline_check_interval}, collector, context)
      {:error, :timeout} = error -> {:halt, error}
    end
  end

  defp collect_item(item, {groups, index, until_check}, collector, _context) do
    case collector.(item, index, groups) do
      {:cont, next_groups} -> {:cont, {next_groups, index + 1, until_check - 1}}
      {:halt, {:error, _reason} = error} -> {:halt, error}
    end
  end

  defp finish({:error, reason}), do: {:error, reason}
  defp finish({groups, _count, _until_check}), do: {:ok, groups}
end
