defmodule FerricStore.SDK.Native.BatchGroupPreparer do
  @moduledoc false

  alias FerricStore.RequestContext
  alias FerricStore.SDK.Native.BatchGroupCallbacks

  @type item_retention :: :retain_items | :discard_items

  @spec prepare(
          [map()],
          (list() -> term()),
          (map() -> {:ok, map()} | {:error, term()}),
          item_retention(),
          RequestContext.t()
        ) :: {:ok, [map()]} | {:error, term()}
  def prepare(groups, payload_builder, group_preparer, item_retention, context)
      when is_list(groups) and is_function(payload_builder, 1) and
             is_function(group_preparer, 1) and
             item_retention in [:retain_items, :discard_items] do
    Enum.reduce_while(groups, {:ok, []}, fn group, {:ok, prepared} ->
      with :ok <- RequestContext.ensure_active(context),
           {:ok, payload} <- BatchGroupCallbacks.build_payload(payload_builder, group.items),
           {:ok, group} <-
             BatchGroupCallbacks.prepare_group(group_preparer, Map.put(group, :payload, payload)),
           :ok <- RequestContext.ensure_active(context) do
        group = retain_items(group, item_retention)
        {:cont, {:ok, [group | prepared]}}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, prepared} -> {:ok, Enum.reverse(prepared)}
      {:error, _reason} = error -> error
    end
  end

  defp retain_items(group, :retain_items), do: group
  defp retain_items(group, :discard_items), do: Map.delete(group, :items)
end
