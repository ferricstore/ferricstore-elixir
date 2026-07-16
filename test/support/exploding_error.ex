defmodule FerricStore.Test.ExplodingError do
  @moduledoc false

  defexception []

  @impl true
  def message(_exception), do: throw(:exploding_exception_message)
end

defmodule FerricStore.Test.RaisingInspect do
  @moduledoc false

  defstruct []
end

defmodule FerricStore.Test.ThrowingInspect do
  @moduledoc false

  defstruct []
end

defimpl Inspect, for: FerricStore.Test.RaisingInspect do
  alias FerricStore.Test.ExplodingError

  def inspect(_value, _opts), do: raise(ExplodingError)
end

defimpl Inspect, for: FerricStore.Test.ThrowingInspect do
  alias FerricStore.Test.ExplodingInspect

  def inspect(_value, _opts), do: throw(%ExplodingInspect{})
end
