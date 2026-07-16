defmodule FerricStore.Test.ExplodingString do
  @moduledoc false

  defstruct []
end

defimpl String.Chars, for: FerricStore.Test.ExplodingString do
  def to_string(_value), do: throw(:exploding_string_conversion)
end
