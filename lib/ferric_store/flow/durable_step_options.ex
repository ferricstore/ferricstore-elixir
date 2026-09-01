defmodule FerricStore.Flow.DurableStepOptions do
  @moduledoc false

  alias FerricStore.Flow.Options.ValueValidator
  alias FerricStore.OptionList

  @max_exact 9_007_199_254_740_991
  @max_options 8
  @transport [:timeout, :call_timeout]
  @advance [:to_state, :lease_ms, :now_ms] ++ @transport
  @step [:name, :run, :to_state, :codec, :lease_ms, :now_ms] ++ @transport

  @spec prepare(:advance | :step, term()) :: {:ok, keyword()} | {:error, term()}
  def prepare(operation, opts) when operation in [:advance, :step] do
    with :ok <- validate_list(operation, opts),
         :ok <- validate_schema(operation, opts),
         :ok <- ValueValidator.validate(operation, opts),
         :ok <- validate_to_state(operation, opts),
         :ok <- validate_step(operation, opts),
         :ok <- validate_timing(operation, opts) do
      {:ok, opts}
    end
  end

  @spec transport_options(keyword()) :: keyword()
  def transport_options(opts), do: Keyword.take(opts, @transport)

  defp validate_list(operation, opts) do
    case OptionList.validate(opts, @max_options) do
      :ok ->
        :ok

      {:error, {:options, {:duplicate_options, keys}}} ->
        {:error, {:duplicate_flow_options, operation, keys}}

      {:error, {:options, {:too_many_options, details}}} ->
        {:error, {:too_many_flow_options, operation, details}}

      {:error, {:options, _invalid}} ->
        {:error, {:invalid_flow_options, operation, :expected_keyword}}
    end
  end

  defp validate_schema(operation, opts) do
    allowed = if operation == :step, do: @step, else: @advance

    case opts |> Keyword.keys() |> Enum.reject(&(&1 in allowed)) |> Enum.sort() do
      [] -> require_options(operation, opts)
      unsupported -> {:error, {:unsupported_flow_options, operation, unsupported}}
    end
  end

  defp require_options(:advance, opts), do: require(:advance, opts, [:to_state])
  defp require_options(:step, opts), do: require(:step, opts, [:name, :run, :to_state])

  defp require(operation, opts, required) do
    case required |> Enum.reject(&Keyword.has_key?(opts, &1)) |> Enum.sort() do
      [] -> :ok
      missing -> {:error, {:missing_flow_options, operation, missing}}
    end
  end

  defp validate_to_state(operation, opts) do
    case Keyword.get(opts, :to_state) do
      state when is_binary(state) and state != "" and state != "running" -> :ok
      "running" -> invalid(operation, :to_state, :reserved_running_state)
      _invalid -> invalid(operation, :to_state, :expected_nonempty_binary)
    end
  end

  defp validate_step(:advance, _opts), do: :ok

  defp validate_step(:step, opts) do
    with :ok <- validate_name(Keyword.get(opts, :name)),
         do: validate_run(Keyword.get(opts, :run))
  end

  defp validate_name(name) when is_binary(name) and name != "" do
    cond do
      not String.valid?(name) -> invalid(:step, :name, :expected_utf8_binary)
      String.trim(name) == "" -> invalid(:step, :name, :expected_nonblank_binary)
      true -> :ok
    end
  end

  defp validate_name(_name), do: invalid(:step, :name, :expected_nonempty_binary)

  defp validate_run(run) when is_function(run, 0), do: :ok
  defp validate_run(_run), do: invalid(:step, :run, :expected_zero_arity_function)

  defp validate_timing(operation, opts) do
    with :ok <- optional_exact(operation, opts, :now_ms, :nonnegative),
         :ok <- optional_exact(operation, opts, :lease_ms, :positive),
         do: validate_deadline(operation, opts)
  end

  defp optional_exact(operation, opts, option, domain) do
    case Keyword.fetch(opts, option) do
      :error ->
        :ok

      {:ok, value} when is_integer(value) and value <= @max_exact ->
        validate_sign(operation, option, value, domain)

      {:ok, _invalid} ->
        invalid(operation, option, exact_expectation(domain))
    end
  end

  defp validate_sign(_operation, _option, value, :nonnegative) when value >= 0, do: :ok
  defp validate_sign(_operation, _option, value, :positive) when value > 0, do: :ok

  defp validate_sign(operation, option, _value, domain),
    do: invalid(operation, option, exact_expectation(domain))

  defp exact_expectation(:nonnegative), do: :expected_nonnegative_exact_integer
  defp exact_expectation(:positive), do: :expected_positive_exact_integer

  defp validate_deadline(operation, opts) do
    case {Keyword.fetch(opts, :now_ms), Keyword.fetch(opts, :lease_ms)} do
      {{:ok, now}, {:ok, lease_ms}} when now > @max_exact - lease_ms ->
        invalid(operation, :lease_ms, {:deadline_exceeds, @max_exact})

      _safe_or_default ->
        :ok
    end
  end

  defp invalid(operation, option, expectation),
    do: {:error, {:invalid_flow_option, operation, option, expectation}}
end
