defmodule FerricStore.Flow.PolicyCommand do
  @moduledoc false

  alias FerricStore.Flow.{
    PolicyNormalizer,
    PolicyStateSelector,
    PolicyStructure,
    PolicyValueValidator
  }

  alias FerricStore.DeadlineBudget

  @spec set_payload(binary(), keyword() | map()) :: {:ok, map()} | {:error, term()}
  def set_payload(type, opts) when is_binary(type) and type != "" do
    with {:ok, options} <- PolicyStructure.set_options(opts),
         :ok <- PolicyValueValidator.validate(options) do
      {:ok, options |> PolicyNormalizer.normalize() |> Map.put("type", type)}
    end
  end

  def set_payload(type, _opts), do: {:error, {:invalid_flow_type, type}}

  @spec set_payload(binary(), keyword() | map(), DeadlineBudget.t()) ::
          {:ok, map()} | {:error, term()}
  def set_payload(type, opts, %DeadlineBudget{} = budget) when is_binary(type) and type != "" do
    with :ok <- DeadlineBudget.ensure_active(budget),
         {:ok, options} <- PolicyStructure.set_options(opts, budget),
         :ok <- PolicyValueValidator.validate(options, budget),
         {:ok, normalized} <- PolicyNormalizer.normalize(options, budget),
         :ok <- DeadlineBudget.ensure_active(budget) do
      {:ok, Map.put(normalized, "type", type)}
    end
  end

  def set_payload(type, _opts, %DeadlineBudget{}), do: {:error, {:invalid_flow_type, type}}

  @spec get_payload(binary(), keyword() | map()) :: {:ok, map()} | {:error, term()}
  def get_payload(type, opts) when is_binary(type) and type != "" do
    with {:ok, options} <- PolicyStructure.get_options(opts),
         :ok <- PolicyStateSelector.validate(options),
         do: {:ok, Map.put(options, "type", type)}
  end

  def get_payload(type, _opts), do: {:error, {:invalid_flow_type, type}}

  @spec get_payload(binary(), keyword() | map(), DeadlineBudget.t()) ::
          {:ok, map()} | {:error, term()}
  def get_payload(type, opts, %DeadlineBudget{} = budget) when is_binary(type) and type != "" do
    with :ok <- DeadlineBudget.ensure_active(budget),
         {:ok, options} <- PolicyStructure.get_options(opts, budget),
         :ok <- PolicyStateSelector.validate(options),
         :ok <- DeadlineBudget.ensure_active(budget),
         do: {:ok, Map.put(options, "type", type)}
  end

  def get_payload(type, _opts, %DeadlineBudget{}), do: {:error, {:invalid_flow_type, type}}
end
