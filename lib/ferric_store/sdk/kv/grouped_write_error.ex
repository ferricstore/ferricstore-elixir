defmodule FerricStore.SDK.KV.GroupedWriteError do
  @moduledoc false

  def invalid_value(%{indexes: []}), do: %{reason: :unexpected_value}

  def invalid_value(%{indexes: [_index | _tail] = indexes}) do
    if proper_list?(indexes),
      do: %{reason: :unexpected_value},
      else: %{reason: :improper_indexes}
  end

  def invalid_value(_group), do: invalid_group()
  def invalid_group, do: %{reason: :unexpected_group_shape}

  def coverage(_operation, :timeout), do: {:error, :timeout}
  def coverage(:del, details), do: {:error, {:invalid_del_group_response, details}}
  def coverage(:mset, details), do: {:error, {:invalid_mset_group_response, details}}

  defp proper_list?([]), do: true
  defp proper_list?([_item | items]), do: proper_list?(items)
  defp proper_list?(_tail), do: false
end
