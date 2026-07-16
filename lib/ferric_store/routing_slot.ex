defmodule FerricStore.RoutingSlot do
  @moduledoc false

  import Bitwise

  @slot_mask 1_023

  @spec for_key(binary()) :: non_neg_integer()
  def for_key("f:{" <> rest = key), do: flow_tag_slot(rest, key)
  def for_key("X:f:{" <> rest = key), do: flow_tag_slot(rest, key)

  def for_key(key) when is_binary(key) do
    key
    |> hash_input()
    |> slot()
  end

  defp flow_tag_slot(rest, fallback_key) do
    case :binary.match(rest, "}") do
      {end_pos, 1} when end_pos > 0 -> rest |> binary_part(0, end_pos) |> slot()
      _missing_tag -> fallback_key |> hash_input() |> slot()
    end
  end

  defp hash_input(key) do
    case :binary.match(key, "{") do
      {start, 1} -> tagged_or_key(key, start + 1)
      :nomatch -> key
    end
  end

  defp tagged_or_key(key, after_open) do
    case :binary.match(binary_part(key, after_open, byte_size(key) - after_open), "}") do
      {end_rel, 1} when end_rel > 0 -> binary_part(key, after_open, end_rel)
      _missing_tag -> key
    end
  end

  defp slot(hash_input), do: :erlang.crc32(hash_input) |> band(@slot_mask)
end
