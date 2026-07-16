defmodule FerricStore.Flow.ConsumerConfig do
  @moduledoc false

  alias FerricStore.{FailureFormatter, OptionList}

  @max_exact_integer 9_007_199_254_740_991
  @max_options 16

  @spec validate!(pid(), binary(), keyword(), keyword()) :: keyword()
  def validate!(client, type, opts, defaults) when is_list(opts) and is_list(defaults) do
    validate_option_list!(opts)
    validate_client!(client)
    validate_nonempty_binary!(:type, type)

    config = Keyword.validate!(opts, defaults)

    config
    |> validate_name!(:state)
    |> validate_name!(:initial_state)
    |> validate_name!(:worker)
    |> validate_lease!()
    |> validate_codec!()

    config
  end

  def validate!(_client, _type, opts, _defaults),
    do:
      raise(
        ArgumentError,
        "expected consumer options to be a keyword list, got: #{FailureFormatter.inspect_term(opts)}"
      )

  defp validate_option_list!(opts) do
    case OptionList.validate(opts, @max_options) do
      :ok ->
        :ok

      {:error, {:options, {:too_many_options, _details}}} ->
        raise ArgumentError, "consumer options exceed #{@max_options} entries"

      {:error, {:options, {:duplicate_options, duplicates}}} ->
        raise ArgumentError,
              "duplicate keys #{FailureFormatter.inspect_term(duplicates)} in consumer options"

      {:error, {:options, _invalid}} ->
        raise ArgumentError, "consumer options must be a keyword list"
    end
  end

  defp validate_client!(client) when is_pid(client), do: :ok

  defp validate_client!(client),
    do:
      raise(
        ArgumentError,
        "expected client to be a pid, got: #{FailureFormatter.inspect_term(client)}"
      )

  defp validate_name!(config, key) do
    case Keyword.fetch(config, key) do
      {:ok, value} -> validate_nonempty_binary!(key, value)
      :error -> :ok
    end

    config
  end

  defp validate_nonempty_binary!(_field, value) when is_binary(value) and value != "", do: :ok

  defp validate_nonempty_binary!(field, value),
    do:
      raise(
        ArgumentError,
        "expected #{field} to be a non-empty binary, got: #{FailureFormatter.inspect_term(value)}"
      )

  defp validate_lease!(config) do
    case Keyword.fetch(config, :lease_ms) do
      {:ok, value} when is_integer(value) and value > 0 and value <= @max_exact_integer ->
        :ok

      {:ok, value} ->
        raise ArgumentError,
              "expected lease_ms to be an exact positive integer, got: #{FailureFormatter.inspect_term(value)}"

      :error ->
        :ok
    end

    config
  end

  defp validate_codec!(config) do
    case Keyword.fetch(config, :codec) do
      {:ok, codec} when is_atom(codec) ->
        if Code.ensure_loaded?(codec) and function_exported?(codec, :encode, 1) and
             function_exported?(codec, :decode, 1) do
          :ok
        else
          raise ArgumentError,
                "expected codec to export encode/1 and decode/1, got: #{FailureFormatter.inspect_term(codec)}"
        end

      {:ok, codec} ->
        raise ArgumentError,
              "expected codec to be a module, got: #{FailureFormatter.inspect_term(codec)}"

      :error ->
        :ok
    end

    config
  end
end
