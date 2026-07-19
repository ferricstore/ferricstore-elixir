defmodule FerricStore.Flow.PolicyResponse do
  @moduledoc false

  alias FerricStore.Error
  alias FerricStore.Flow.{PolicySnapshot, StalePolicyGenerationError}
  alias FerricStore.Types

  @stale_message "ERR stale flow policy generation"

  @spec decode(term(), binary(), map()) :: {:ok, PolicySnapshot.t()} | {:error, term()}
  def decode({:ok, value}, type, _request_payload), do: PolicySnapshot.decode(value, type)

  def decode({:error, reason} = error, _type, request_payload) do
    case stale_message(reason) do
      {:ok, message} ->
        {:error,
         %StalePolicyGenerationError{
           message: message,
           expected_generation: Types.get(request_payload, "expected_generation"),
           raw: reason
         }}

      :error ->
        error
    end
  end

  def decode(other, _type, _request_payload),
    do: {:error, {:invalid_policy_response, other}}

  defp stale_message(%Error{raw: raw}), do: stale_message(raw)
  defp stale_message({_status, payload}), do: stale_message(payload)

  defp stale_message(payload) when is_map(payload) do
    case Types.get(payload, "message") do
      @stale_message = message -> {:ok, message}
      _other -> :error
    end
  end

  defp stale_message(@stale_message = message), do: {:ok, message}
  defp stale_message(_reason), do: :error
end
