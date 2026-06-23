defmodule FerricStore.Error do
  @moduledoc """
  Raised when FerricStore returns a protocol or command error.
  """

  defexception [:message, :status, :raw]
end
