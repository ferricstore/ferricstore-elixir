defmodule FerricStore.Flow.QueryError do
  @moduledoc """
  Actionable, value-redacted FQL diagnostic returned by FerricStore.
  """

  defexception [
    :code,
    :message,
    :detail,
    :hint,
    :retryable,
    :safe_to_retry,
    :retry_after_ms,
    :position,
    :context,
    :raw
  ]

  @type t :: %__MODULE__{
          code: binary(),
          message: binary(),
          detail: binary() | nil,
          hint: binary() | nil,
          retryable: boolean(),
          safe_to_retry: boolean(),
          retry_after_ms: non_neg_integer(),
          position: %{byte: pos_integer(), line: pos_integer(), column: pos_integer()} | nil,
          context: map() | nil,
          raw: term()
        }
end
