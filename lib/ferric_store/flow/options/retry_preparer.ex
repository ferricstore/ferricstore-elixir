defmodule FerricStore.Flow.Options.RetryPreparer do
  @moduledoc false

  alias FerricStore.DeadlineBudget
  alias FerricStore.Flow.Options.PreparedMap
  alias FerricStore.Flow.{PolicyNormalizer, PolicyStructure, PolicyValueValidator}

  @spec prepare(atom(), keyword()) :: {:ok, keyword()} | {:error, term()}
  def prepare(operation, opts) when operation != :retry, do: {:ok, opts}

  def prepare(:retry, opts) do
    case Keyword.fetch(opts, :retry) do
      :error -> {:ok, opts}
      {:ok, nil} -> {:ok, opts}
      {:ok, value} -> prepare_value(opts, value)
    end
  end

  @spec prepare(atom(), keyword(), DeadlineBudget.t()) :: {:ok, keyword()} | {:error, term()}
  def prepare(operation, opts, %DeadlineBudget{} = budget) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         {:ok, prepared} <- prepare(operation, opts),
         :ok <- DeadlineBudget.ensure_active(budget),
         do: {:ok, prepared}
  end

  defp prepare_value(opts, value) do
    with {:ok, policy} <- PolicyStructure.set_options(retry: value),
         :ok <- PolicyValueValidator.validate(policy) do
      normalized = policy |> PolicyNormalizer.normalize() |> Map.fetch!("retry")
      {:ok, Keyword.replace!(opts, :retry, PreparedMap.new(normalized))}
    else
      {:error, reason} -> invalid(reason)
    end
  end

  defp invalid(reason), do: {:error, {:invalid_flow_option, :retry, :retry, reason}}
end
