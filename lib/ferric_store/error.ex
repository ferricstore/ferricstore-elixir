defmodule FerricStore.Error do
  @moduledoc """
  Raised when FerricStore returns a protocol or command error.
  """

  defexception [:message, :status, :raw]

  @type t :: %__MODULE__{message: binary(), status: atom() | nil, raw: term()}
end
