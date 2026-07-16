defmodule FerricStore.SDK.InvocationOptions do
  @moduledoc false

  alias FerricStore.RequestOptions
  alias FerricStore.SDK.InvocationError

  @request_option_keys ~w(
    timeout call_timeout idempotent lane_id request_context endpoint key
  )a

  @spec validate(term(), [atom()]) :: :ok | {:error, term()}
  def validate(opts, operation_options \\ []) do
    case RequestOptions.validate_supported(opts, @request_option_keys ++ operation_options) do
      :ok -> :ok
      {:error, {key, value}} -> {:error, {:invalid_request_option, key, value}}
    end
  end

  @spec optional_binary(keyword(), atom(), atom()) :: :ok | {:error, term()}
  def optional_binary(opts, key, operation) do
    case Keyword.get(opts, key) do
      nil -> :ok
      value when is_binary(value) -> :ok
      value -> InvocationError.invalid(operation, key, :expected_binary, value)
    end
  end

  @spec scope(keyword()) :: {:ok, binary() | nil} | {:error, term()}
  def scope(opts) do
    case Keyword.get(opts, :scope) do
      nil -> {:ok, nil}
      value when is_binary(value) -> {:ok, value}
      value -> InvocationError.invalid(:list_partitions, :scope, :expected_binary, value)
    end
  end
end
