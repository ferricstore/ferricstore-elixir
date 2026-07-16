defmodule FerricStore.Test.ObservableString do
  @moduledoc false

  defstruct [:owner, value: "converted"]
end

defimpl String.Chars, for: FerricStore.Test.ObservableString do
  def to_string(%{owner: owner, value: value}) do
    send(owner, :string_chars_called)
    value
  end
end
