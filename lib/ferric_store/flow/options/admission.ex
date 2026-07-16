defmodule FerricStore.Flow.Options.Admission do
  @moduledoc false

  alias FerricStore.Flow.Options.{MutationSchema, QuerySchema}
  alias FerricStore.OptionList

  @max_options 64
  @mutation_operations ~w(create create_many transition complete complete_many retry fail cancel signal)a

  @spec validate(atom(), term()) :: :ok | {:error, term()}
  def validate(operation, opts) do
    with :ok <- validate_list(operation, opts),
         :ok <- reject_unsupported(operation, opts),
         :ok <- require_options(operation, opts),
         do: reject_conflicting_options(operation, opts)
  end

  @spec validate_noop(atom(), term()) :: :ok | {:error, term()}
  def validate_noop(operation, opts) do
    with :ok <- validate_list(operation, opts),
         :ok <- reject_unsupported(operation, opts),
         do: reject_conflicting_options(operation, opts)
  end

  @spec validate_list(atom(), term()) :: :ok | {:error, term()}
  def validate_list(operation, opts) do
    case OptionList.validate(opts, @max_options) do
      :ok ->
        :ok

      {:error, {:options, {:duplicate_options, duplicates}}} ->
        {:error, {:duplicate_flow_options, operation, duplicates}}

      {:error, {:options, {:too_many_options, details}}} ->
        {:error, {:too_many_flow_options, operation, details}}

      {:error, {:options, _invalid}} ->
        {:error, {:invalid_flow_options, operation, :expected_keyword}}
    end
  end

  defp reject_unsupported(operation, _opts) when operation in [:policy_set, :policy_get], do: :ok

  defp reject_unsupported(operation, opts) do
    {_required, allowed} = schema(operation)
    allowed = MapSet.new(allowed)

    unsupported =
      opts
      |> Keyword.keys()
      |> Enum.reject(&MapSet.member?(allowed, &1))
      |> Enum.uniq()
      |> Enum.sort()

    if unsupported == [],
      do: :ok,
      else: {:error, {:unsupported_flow_options, operation, unsupported}}
  end

  defp require_options(operation, opts) do
    {required, _allowed} = schema(operation)
    missing = required |> Enum.reject(&Keyword.has_key?(opts, &1)) |> Enum.sort()

    if missing == [],
      do: :ok,
      else: {:error, {:missing_flow_options, operation, missing}}
  end

  defp reject_conflicting_options(:claim_due, opts) do
    cond do
      Keyword.has_key?(opts, :state) and Keyword.has_key?(opts, :states) ->
        {:error, {:conflicting_flow_options, :claim_due, [:state, :states]}}

      Keyword.has_key?(opts, :partition_key) and Keyword.has_key?(opts, :partition_keys) ->
        {:error, {:conflicting_flow_options, :claim_due, [:partition_key, :partition_keys]}}

      true ->
        :ok
    end
  end

  defp reject_conflicting_options(_operation, _opts), do: :ok

  defp schema(operation) when operation in @mutation_operations,
    do: MutationSchema.fetch!(operation)

  defp schema(operation), do: QuerySchema.fetch!(operation)
end
