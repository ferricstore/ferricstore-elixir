defmodule FerricStore.Flow.ChildResult do
  @moduledoc """
  One child of a group: its status and, once terminal, its decoded result or
  error.

  `status` is `"running"`, `"completed"`, `"failed"`, or `"cancelled"`.
  """

  defstruct [:id, :partition_key, :status, :result, :error]

  @type t :: %__MODULE__{
          id: binary(),
          partition_key: binary() | nil,
          status: binary(),
          result: term(),
          error: term()
        }
end
