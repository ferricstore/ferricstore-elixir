defmodule FerricStore.BinaryDetacher do
  @moduledoc false

  @spec detach(binary()) :: binary()
  def detach(value) when is_binary(value) do
    referenced_size = :binary.referenced_byte_size(value)
    value_size = byte_size(value)

    if referenced_size > max(value_size * 2, 64), do: :binary.copy(value), else: value
  end
end
