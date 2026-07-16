defmodule FerricStore.AsyncRequest do
  @moduledoc """
  Handle for an asynchronous FerricStore request.

  The handle retains the client and owning process so a timed-out wait can
  cancel the corresponding coordinator and connection work deterministically.
  """

  @enforce_keys [:client, :source, :ref, :owner]
  defstruct [:client, :source, :ref, :owner]

  @type t :: %__MODULE__{client: pid(), source: pid(), ref: reference(), owner: pid()}
end
