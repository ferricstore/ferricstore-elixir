defmodule FerricStore.OptionList do
  @moduledoc false

  alias FerricStore.BoundedList

  @spec validate(term(), pos_integer()) :: :ok | {:error, {:options, term()}}
  def validate(opts, max_options)
      when is_list(opts) and is_integer(max_options) and max_options > 0 do
    case BoundedList.count(opts, max_options) do
      {:ok, _count} -> validate_keyword(opts)
      {:error, {:limit_exceeded, observed}} -> too_many(max_options, observed)
      {:error, :improper_list} -> invalid(opts)
    end
  end

  def validate(opts, _max_options), do: invalid(opts)

  defp validate_keyword(opts) do
    if Keyword.keyword?(opts), do: validate_unique(opts), else: invalid(opts)
  end

  defp validate_unique(opts) do
    {_seen, duplicates} =
      Enum.reduce(opts, {MapSet.new(), MapSet.new()}, fn {key, _value}, {seen, duplicates} ->
        if MapSet.member?(seen, key),
          do: {seen, MapSet.put(duplicates, key)},
          else: {MapSet.put(seen, key), duplicates}
      end)

    case duplicates |> MapSet.to_list() |> Enum.sort() do
      [] -> :ok
      keys -> {:error, {:options, {:duplicate_options, keys}}}
    end
  end

  defp too_many(limit, observed),
    do: {:error, {:options, {:too_many_options, %{limit: limit, observed: observed}}}}

  defp invalid(opts), do: {:error, {:options, opts}}
end
