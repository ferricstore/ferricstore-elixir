defmodule FerricStore.Flow.Options.PreparedValues do
  @moduledoc false

  defstruct [:value]

  @type t :: %__MODULE__{value: map()}

  @spec new(map()) :: t()
  def new(value) when is_map(value), do: %__MODULE__{value: value}
end
