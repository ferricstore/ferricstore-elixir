defmodule FerricStore.SDK.InvocationInput do
  @moduledoc false

  alias FerricStore.DeadlineBudget
  alias FerricStore.SDK.{InvocationError, InvocationJSON}

  @spec nonempty_binary(term(), atom(), atom()) :: {:ok, binary()} | {:error, term()}
  def nonempty_binary(value, _operation, _field) when is_binary(value) and value != "",
    do: {:ok, value}

  def nonempty_binary(value, operation, field),
    do: InvocationError.invalid(operation, field, :expected_nonempty_binary, value)

  @spec map(term(), atom(), atom()) :: {:ok, map()} | {:error, term()}
  def map(value, _operation, _field) when is_map(value), do: {:ok, value}

  def map(value, operation, field),
    do: InvocationError.invalid(operation, field, :expected_map, value)

  @spec definition(term(), DeadlineBudget.t()) :: {:ok, binary()} | {:error, term()}
  def definition(value, %DeadlineBudget{} = budget) when is_binary(value) do
    case InvocationJSON.validate_object(value, budget) do
      :ok ->
        {:ok, value}

      {:error, :timeout} = error ->
        error

      {:error, reason} ->
        InvocationError.invalid(:put_definition, :definition, reason, :redacted)
    end
  end

  def definition(value, %DeadlineBudget{} = budget) when is_map(value),
    do: json(value, :put_definition, :definition, budget)

  def definition(value, %DeadlineBudget{} = budget) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         do:
           InvocationError.invalid(
             :put_definition,
             :definition,
             :expected_map_or_binary,
             value
           )
  end

  @spec json(term(), atom(), atom(), DeadlineBudget.t()) ::
          {:ok, binary()} | {:error, term()}
  def json(value, operation, field, %DeadlineBudget{} = budget) do
    case InvocationJSON.encode(value, budget) do
      {:ok, encoded} ->
        {:ok, encoded}

      {:error, :timeout} = error ->
        error

      {:error, :not_json_encodable} ->
        InvocationError.invalid(operation, field, :not_json_encodable, value)
    end
  end
end
