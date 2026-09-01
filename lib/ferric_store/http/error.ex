defmodule FerricStore.HTTP.Error do
  @moduledoc """
  A structured HTTP transport, gateway, or command error.

  `safe_to_retry` is deliberately false for uncertain POST failures unless the
  endpoint explicitly proves otherwise.
  """

  defexception [
    :message,
    :status_code,
    :error_code,
    :retry_after_ms,
    :details,
    :reason,
    retryable: false,
    safe_to_retry: false,
    delivery: :unknown
  ]

  @type t :: %__MODULE__{
          message: binary(),
          status_code: non_neg_integer() | nil,
          error_code: binary() | nil,
          retry_after_ms: non_neg_integer() | nil,
          details: map() | nil,
          reason: term(),
          retryable: boolean(),
          safe_to_retry: boolean(),
          delivery: :not_sent | :rejected | :unknown
        }

  @spec timeout(:not_sent | :unknown) :: {:error, t()}
  def timeout(delivery \\ :unknown) when delivery in [:not_sent, :unknown] do
    {:error,
     %__MODULE__{
       message: "HTTP command request timed out",
       reason: :timeout,
       retryable: true,
       safe_to_retry: delivery == :not_sent,
       delivery: delivery
     }}
  end

  @spec network(term()) :: {:error, t()}
  def network(reason) do
    {:error,
     %__MODULE__{
       message: "HTTP command request failed",
       reason: reason,
       retryable: true,
       safe_to_retry: false,
       delivery: :unknown
     }}
  end

  @spec invalid(term(), :not_sent | :unknown) :: {:error, t()}
  def invalid(reason, delivery \\ :not_sent) when delivery in [:not_sent, :unknown] do
    {:error,
     %__MODULE__{
       message: "invalid HTTP command request",
       reason: reason,
       safe_to_retry: delivery == :not_sent,
       delivery: delivery
     }}
  end
end
