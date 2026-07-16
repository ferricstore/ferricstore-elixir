defmodule FerricStore.Flow.Options.PreparedValue do
  @moduledoc false

  defstruct [:value]

  @type t :: %__MODULE__{value: binary()}

  @spec new(binary()) :: t()
  def new(value) when is_binary(value), do: %__MODULE__{value: value}
end
