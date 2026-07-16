defmodule FerricStore.SDK.Native.BatchGroupCallbacks do
  @moduledoc false

  alias FerricStore.FailureFormatter
  alias FerricStore.SDK.Native.ClientRequestAdmission

  @spec build_payload((list() -> term()), list()) :: {:ok, map()} | {:error, term()}
  def build_payload(payload_builder, items) do
    payload = payload_builder.(items)

    case ClientRequestAdmission.validate_external_payload(payload) do
      :ok when is_map(payload) -> {:ok, payload}
      :ok -> {:error, {:invalid_batch_payload, payload}}
      {:error, {:invalid_request_payload, details}} -> {:error, {:invalid_batch_payload, details}}
    end
  rescue
    error ->
      {:error,
       {:payload_builder_failed,
        {:error, FailureFormatter.exception_message(error, "payload builder failed")}}}
  catch
    kind, reason -> {:error, {:payload_builder_failed, {kind, reason}}}
  end

  @spec prepare_group((map() -> term()), map()) :: {:ok, map()} | {:error, term()}
  def prepare_group(group_preparer, group) do
    case group_preparer.(group) do
      {:ok, prepared} when is_map(prepared) -> {:ok, prepared}
      {:error, _reason} = error -> error
      other -> {:error, {:invalid_group_preparation_result, other}}
    end
  rescue
    error ->
      {:error,
       {:group_preparation_failed,
        FailureFormatter.exception_message(error, "group preparation failed")}}
  catch
    kind, reason -> {:error, {:group_preparation_failed, {kind, reason}}}
  end
end
