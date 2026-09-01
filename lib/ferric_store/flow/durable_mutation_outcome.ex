defmodule FerricStore.Flow.DurableMutationOutcome do
  @moduledoc false

  alias FerricStore.Flow.DurableMutationOutcomeUnknownError
  alias FerricStore.{Result, Types}

  @known_rejection_codes ~w(
    auth unauthenticated unauthorized noperm forbidden bad_request invalid_command
    invalid_request not_found flow_not_found stale_lease wrong_state conflict request_too_large
  )

  @spec from_error({:error, term()}) :: {:error, Exception.t()}
  def from_error(error) do
    if definitely_rejected?(error), do: normalize(error), else: unknown(error)
  end

  @spec unknown({:error, term()}) :: {:error, DurableMutationOutcomeUnknownError.t()}
  def unknown(error) do
    {:error, cause} = normalize(error)

    {:error,
     DurableMutationOutcomeUnknownError.exception(
       operation: :flow_step_continue,
       cause: cause
     )}
  end

  @spec normalize(term()) :: {:error, Exception.t()}
  def normalize(error) do
    case Result.unwrap(error) do
      {:error, _reason} = normalized -> normalized
      other -> {:error, other}
    end
  end

  defp definitely_rejected?({:error, reason}), do: definitely_rejected_reason?(reason)

  defp definitely_rejected_reason?(%FerricStore.Error{status: status})
       when status in [:auth, :noperm, :busy, :reroute, :bad_request],
       do: true

  defp definitely_rejected_reason?(%FerricStore.Error{status: :error, raw: raw}),
    do: known_rejection?(raw)

  defp definitely_rejected_reason?(%FerricStore.Error{raw: raw}),
    do: definitely_rejected_reason?(raw)

  defp definitely_rejected_reason?(%FerricStore.HTTP.Error{delivery: delivery})
       when delivery in [:not_sent, :rejected],
       do: true

  defp definitely_rejected_reason?(%FerricStore.HTTP.Error{}), do: false

  defp definitely_rejected_reason?({status, _value})
       when status in [:auth, :noperm, :busy, :reroute, :bad_request],
       do: true

  defp definitely_rejected_reason?({:error, value}), do: known_rejection?(value)

  defp definitely_rejected_reason?(reason)
       when reason in [
              :client_backpressure,
              :connection_backpressure,
              :connection_draining,
              :duplicate_request_target,
              :request_too_large
            ],
       do: true

  defp definitely_rejected_reason?({:encode_failed, _reason}), do: true
  defp definitely_rejected_reason?({:invalid_request_payload, _reason}), do: true
  defp definitely_rejected_reason?({:invalid_request_option, _option, _value}), do: true

  defp definitely_rejected_reason?(reason) when is_map(reason) and not is_struct(reason),
    do: known_rejection?(reason)

  defp definitely_rejected_reason?(_reason), do: false

  defp known_rejection?(value) when is_map(value) do
    Types.get(value, "safe_to_retry") == true or
      Types.get(value, "code") in @known_rejection_codes or
      Types.get(value, "error_code") in @known_rejection_codes or
      known_rejection?(Types.get(value, "message"))
  end

  defp known_rejection?(value) when is_binary(value) do
    message = String.downcase(value)

    Enum.any?(
      ["stale lease", "stale flow lease", "wrong state", "flow not found"],
      &String.contains?(message, &1)
    )
  end

  defp known_rejection?(_value), do: false
end
