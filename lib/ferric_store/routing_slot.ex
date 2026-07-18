defmodule FerricStore.RoutingSlot do
  @moduledoc false

  import Bitwise

  @slot_mask 1_023

  @spec for_key(binary()) :: non_neg_integer()
  def for_key(key) when is_binary(key) do
    key
    |> hash_input()
    |> slot()
  end

  @spec for_tag(binary()) :: non_neg_integer()
  def for_tag(tag) when is_binary(tag), do: slot(tag)

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
