defmodule FerricStore.Test.ExplodingJSON do
  @moduledoc false
  defstruct []
end

defimpl Jason.Encoder, for: FerricStore.Test.ExplodingJSON do
  def encode(_value, _opts), do: raise("encoder failed")
end
