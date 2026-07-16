defmodule FerricStore.Flow.PolicyOptionStructure do
  @moduledoc false

  alias FerricStore.{OptionList, Types}

  @retry_options ~w(max_retries backoff exhausted_to)
  @backoff_options ~w(kind base_ms max_ms jitter_pct)
  @retention_options ~w(ttl_ms history_max_events)
  @state_options ~w(mode retry retention)
  @max_nested_options 16

  def option_map(opts) when is_map(opts) and map_size(opts) <= @max_nested_options,
    do: Types.normalize_map_keys_result(opts)

  def option_map(opts) when is_map(opts),
    do: {:error, {:invalid_policy_options, :too_many_options}}

  def option_map(opts) when is_list(opts) do
    case OptionList.validate(opts, @max_nested_options) do
      :ok -> opts |> Map.new() |> Types.normalize_map_keys_result()
      {:error, {:options, _reason}} -> {:error, {:invalid_policy_options, :expected_options}}
    end
  end

  def option_map(_opts), do: {:error, {:invalid_policy_options, :expected_options}}

  def validate_options(options, supported) do
    unsupported = options |> Map.keys() |> Enum.reject(&(&1 in supported)) |> Enum.sort()

    if unsupported == [],
      do: :ok,
      else: {:error, {:unsupported_policy_options, unsupported}}
  end

  def validate_retry(nil), do: :ok

  def validate_retry(retry) do
    with {:ok, retry} <- nested_option_map(retry, "retry"),
         :ok <- validate_options(retry, @retry_options) do
      validate_backoff(Map.get(retry, "backoff"))
    end
  end

  def validate_retention(nil), do: :ok

  def validate_retention(retention) do
    with {:ok, retention} <- nested_option_map(retention, "retention") do
      validate_options(retention, @retention_options)
    end
  end

  def validate_state_policy(policy) do
    with {:ok, policy} <- nested_option_map(policy, "state policy"),
         :ok <- validate_options(policy, @state_options),
         :ok <- validate_retry(Map.get(policy, "retry")) do
      validate_retention(Map.get(policy, "retention"))
    end
  end

  defp validate_backoff(nil), do: :ok

  defp validate_backoff(backoff) do
    with {:ok, backoff} <- nested_option_map(backoff, "retry.backoff") do
      validate_options(backoff, @backoff_options)
    end
  end

  defp nested_option_map(value, name) do
    case option_map(value) do
      {:ok, options} -> {:ok, options}
      {:error, _reason} -> {:error, {:invalid_policy_option, name}}
    end
  end
end
