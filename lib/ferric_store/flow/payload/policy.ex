defmodule FerricStore.Flow.Payload.Policy do
  @moduledoc false

  import FerricStore.Flow.Payload.Normalize,
    only: [stringify_nested_map: 1]

  alias FerricStore.Flow.Options.PreparedMap
  alias FerricStore.Types

  def normalize_search_state_meta(opts) do
    state_meta = opts |> Keyword.get(:state_meta) |> PreparedMap.unwrap()

    case {Keyword.get(opts, :state), state_meta} do
      {_state, nil} ->
        nil

      {state, state_meta}
      when is_binary(state) and state not in ["", "any"] and is_map(state_meta) ->
        normalize_state_scoped_meta(state, state_meta)

      {_state, state_meta} ->
        stringify_nested_map(state_meta)
    end
  end

  def normalize_policy_value(nil), do: nil
  def normalize_policy_value(%PreparedMap{value: value}), do: value

  def normalize_policy_value(values) when is_list(values) do
    cond do
      Keyword.keyword?(values) ->
        values |> normalized_pair_map() |> normalize_policy_value()

      state_policy_pair_list?(values) ->
        values |> normalized_pair_map() |> normalize_policy_value()

      true ->
        Enum.map(values, &normalize_policy_value/1)
    end
  end

  def normalize_policy_value(map) when is_map(map) do
    map
    |> Types.normalize_map()
    |> Map.new(fn {key, value} -> {key, normalize_policy_value(value)} end)
  end

  def normalize_policy_value(value), do: value

  defp state_policy_pair_list?([_ | _] = values), do: Enum.all?(values, &state_policy_pair?/1)

  defp state_policy_pair?({state, policy})
       when (is_binary(state) or is_atom(state)) and (is_map(policy) or is_list(policy)),
       do: true

  defp state_policy_pair?(_value), do: false

  defp normalized_pair_map(values) do
    Enum.reduce(values, %{}, fn {key, value}, acc ->
      key = to_string(key)

      if Map.has_key?(acc, key) do
        raise ArgumentError, "duplicate normalized map key #{inspect(key)}"
      else
        Map.put(acc, key, value)
      end
    end)
  end

  defp normalize_state_scoped_meta(state, state_meta) do
    if state_scoped_meta?(state_meta) do
      stringify_nested_map(state_meta)
    else
      %{state => stringify_nested_map(state_meta)}
    end
  end

  defp state_scoped_meta?(state_meta) do
    Enum.all?(state_meta, fn {_key, value} -> is_map(value) end)
  end
end
