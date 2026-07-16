defmodule FerricStore.SDK.KV.Options do
  @moduledoc false

  alias FerricStore.{RequestContext, RequestOptions}
  alias FerricStore.SDK.KV.Input

  @default_timeout 5_000

  @boolean_set_options [:nx, :xx, :get, :keepttl]
  @shared_options [:timeout, :call_timeout, :idempotent]
  @operation_options %{
    set: [:ttl, :exat, :pxat | @boolean_set_options],
    cas: [:ttl],
    del: [:atomicity, :max_group_concurrency],
    mget: [:max_group_concurrency],
    mset: [:atomicity, :max_group_concurrency],
    fetch_or_compute: [:hint],
    zrange: [:withscores]
  }

  @spec validate(atom(), keyword()) :: {:ok, RequestContext.t()} | {:error, term()}
  def validate(operation, opts) when is_atom(operation) and is_list(opts) do
    with :ok <- validate_request_options(operation, opts),
         :ok <- validate_supported_options(operation, opts),
         :ok <- validate_operation_options(operation, opts),
         context = RequestContext.new(opts, @default_timeout),
         :ok <- RequestContext.ensure_active(context) do
      {:ok, context}
    end
  end

  def validate(operation, opts) when is_atom(operation),
    do: invalid_input(operation, :options, :expected_keyword, %{value: opts})

  defp validate_operation_options(:set, opts), do: validate_set(opts)
  defp validate_operation_options(:cas, opts), do: validate_cas(opts)
  defp validate_operation_options(:del, opts), do: validate_atomicity(:del, opts)
  defp validate_operation_options(:mset, opts), do: validate_atomicity(:mset, :per_slot, opts)

  defp validate_operation_options(:fetch_or_compute, opts),
    do: validate_optional(opts, :hint, &Input.binary(&1, :fetch_or_compute, :hint))

  defp validate_operation_options(:zrange, opts),
    do:
      opts
      |> Keyword.get(:withscores)
      |> Input.optional_boolean(:zrange, :withscores)
      |> normalize_validation()

  defp validate_operation_options(_operation, _opts), do: :ok

  defp validate_set(opts) do
    with :ok <- validate_optional(opts, :ttl, &Input.non_negative_integer(&1, :set, :ttl)),
         :ok <- validate_optional(opts, :exat, &Input.positive_integer(&1, :set, :exat)),
         :ok <- validate_optional(opts, :pxat, &Input.positive_integer(&1, :set, :pxat)),
         :ok <- validate_set_booleans(opts),
         :ok <- validate_set_conditions(opts) do
      validate_set_expiry(opts)
    end
  end

  defp validate_cas(opts),
    do: validate_optional(opts, :ttl, &Input.non_negative_integer(&1, :cas, :ttl))

  defp validate_atomicity(:del, opts), do: validate_atomicity(:del, :per_shard, opts)

  defp validate_atomicity(operation, policy, opts) do
    case Keyword.get(opts, :atomicity) do
      value when value in [nil, policy] ->
        :ok

      value ->
        invalid_input(operation, :atomicity, expected_policy(policy), %{value: value})
    end
  end

  defp expected_policy(:per_shard), do: :expected_per_shard
  defp expected_policy(:per_slot), do: :expected_per_slot

  defp validate_set_booleans(opts) do
    Enum.reduce_while(@boolean_set_options, :ok, fn key, :ok ->
      case validate_optional(opts, key, &Input.optional_boolean(&1, :set, key)) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp validate_set_conditions(opts) do
    if Keyword.get(opts, :nx) == true and Keyword.get(opts, :xx) == true do
      invalid_input(:set, :conditions, :mutually_exclusive, %{options: [:nx, :xx]})
    else
      :ok
    end
  end

  defp validate_set_expiry(opts) do
    enabled =
      [:ttl, :exat, :pxat]
      |> Enum.filter(&(not is_nil(Keyword.get(opts, &1))))
      |> maybe_add_keepttl(opts)

    if length(enabled) > 1 do
      invalid_input(:set, :expiry, :mutually_exclusive, %{options: enabled})
    else
      :ok
    end
  end

  defp maybe_add_keepttl(options, opts) do
    if Keyword.get(opts, :keepttl) == true, do: options ++ [:keepttl], else: options
  end

  defp validate_optional(opts, key, validator) do
    case Keyword.get(opts, key) do
      nil -> :ok
      value -> validator.(value) |> normalize_validation()
    end
  end

  defp validate_request_options(operation, opts) do
    case RequestOptions.validate(opts) do
      :ok ->
        :ok

      {:error, {:options, {:duplicate_options, duplicates}}} ->
        invalid_input(operation, :options, :duplicate_options, %{options: duplicates})

      {:error, {key, value}} ->
        {:error, {:invalid_request_option, key, value}}
    end
  end

  defp validate_supported_options(operation, opts) do
    allowed = @shared_options ++ Map.get(@operation_options, operation, [])

    unsupported =
      opts
      |> Keyword.keys()
      |> Enum.uniq()
      |> Enum.reject(&(&1 in allowed))
      |> Enum.sort()

    if unsupported == [],
      do: :ok,
      else: invalid_input(operation, :options, :unsupported_options, %{options: unsupported})
  end

  defp normalize_validation({:ok, _value}), do: :ok
  defp normalize_validation({:error, _reason} = error), do: error

  defp invalid_input(operation, field, reason, details) do
    {:error,
     {:invalid_kv_input,
      Map.merge(%{operation: operation, field: field, reason: reason}, details)}}
  end
end
