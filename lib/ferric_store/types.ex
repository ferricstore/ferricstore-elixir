defmodule FerricStore.Types do
  @moduledoc """
  Helpers for reading native protocol maps returned by FerricStore.
  """

  def get(map, key, default \\ nil)

  def get(map, key, default) when is_map(map) and is_atom(key),
    do: get(map, Atom.to_string(key), default)

  def get(map, key, default) when is_map(map) and is_binary(key),
    do: Map.get(map, key, Map.get(map, String.to_atom(key), default))

  def get(_map, _key, default), do: default

  def normalize_map(value) when is_map(value) do
    Map.new(value, fn {key, item} -> {normalize_key(key), normalize_map(item)} end)
  end

  def normalize_map(value) when is_list(value), do: Enum.map(value, &normalize_map/1)
  def normalize_map(value), do: value

  defp normalize_key(key) when is_binary(key), do: key
  defp normalize_key(key) when is_atom(key), do: Atom.to_string(key)
  defp normalize_key(key), do: to_string(key)
end
