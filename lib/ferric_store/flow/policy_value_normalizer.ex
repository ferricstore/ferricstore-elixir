defmodule FerricStore.Flow.PolicyValueNormalizer do
  @moduledoc false

  alias FerricStore.Types

  @array_options ~w(indexed_attributes indexed_state_meta)

  def normalize(key, values) when key in @array_options and is_list(values),
    do: Enum.map(values, &normalize(nil, &1))

  def normalize(_key, values) when is_list(values) do
    cond do
      Keyword.keyword?(values) -> values |> normalized_map() |> normalize_map()
      state_policy_pairs?(values) -> values |> normalized_map() |> normalize_map()
      true -> Enum.map(values, &normalize(nil, &1))
    end
  end

  def normalize(_key, map) when is_map(map),
    do: map |> Types.normalize_map() |> normalize_map()

  def normalize(_key, value), do: value

  defp normalize_map(map),
    do: Map.new(map, fn {key, value} -> {key, normalize(key, value)} end)

  defp normalized_map(values), do: values |> Map.new() |> Types.normalize_map()

  defp state_policy_pairs?([_ | _] = values) do
    Enum.all?(values, fn
      {state, _policy} when is_binary(state) or is_atom(state) -> true
      _other -> false
    end)
  end
end
