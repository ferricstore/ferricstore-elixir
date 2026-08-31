defmodule FerricStore.Flow.DurableMutationOutcome do
  @moduledoc false

  alias FerricStore.Flow.DurableMutationOutcomeUnknownError
  alias FerricStore.{Result, Types}

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

  defp definitely_rejected_reason?(%FerricStore.Error{status: status, raw: raw})
       when status in [:busy, :reroute] and is_map(raw),
       do: Types.get(raw, "safe_to_retry") == true

  defp definitely_rejected_reason?(%FerricStore.Error{status: status})
       when status in [:error, :auth, :noperm, :bad_request],
       do: true

  defp definitely_rejected_reason?(%FerricStore.Error{raw: raw}),
    do: definitely_rejected_reason?(raw)

  defp definitely_rejected_reason?(%FerricStore.HTTP.Error{safe_to_retry: true}), do: true

  defp definitely_rejected_reason?({status, value})
       when status in [:busy, :reroute] and is_map(value),
       do: Types.get(value, "safe_to_retry") == true

  defp definitely_rejected_reason?({status, _value})
       when status in [:error, :auth, :noperm, :bad_request],
       do: true

  defp definitely_rejected_reason?(reason) when is_map(reason) and not is_struct(reason), do: true
  defp definitely_rejected_reason?(_reason), do: false
end
