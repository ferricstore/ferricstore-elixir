defmodule FerricStore.Test.ExplodingInspect do
  @moduledoc false

  defstruct []
end

defimpl Inspect, for: FerricStore.Test.ExplodingInspect do
  def inspect(_value, _opts), do: throw(:exploding_inspect)
end
