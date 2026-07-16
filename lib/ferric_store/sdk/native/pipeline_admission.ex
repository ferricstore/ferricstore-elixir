defmodule FerricStore.SDK.Native.PipelineAdmission do
  @moduledoc false

  alias FerricStore.{BoundedList, DeadlineBudget}
  alias FerricStore.Protocol.PipelineCommand

  @deadline_check_interval 256

  @spec admit(list(), non_neg_integer(), DeadlineBudget.t()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def admit(commands, limit, %DeadlineBudget{} = budget)
      when is_list(commands) and is_integer(limit) and limit >= 0 do
    with {:ok, count} <- count(commands, limit, budget),
         :ok <- validate(commands, 0, 0, budget) do
      {:ok, count}
    end
  end

  defp count(commands, limit, budget) do
    case BoundedList.count(commands, limit, budget) do
      {:ok, count} ->
        {:ok, count}

      {:error, {:limit_exceeded, observed}} ->
        {:error, {:pipeline_too_large, %{items: observed, limit: limit}}}

      {:error, :improper_list} ->
        {:error, {:invalid_pipeline, :improper_list}}

      {:error, :timeout} = error ->
        error
    end
  end

  defp validate(commands, index, 0, budget) do
    with :ok <- DeadlineBudget.ensure_active(budget) do
      validate(commands, index, @deadline_check_interval, budget)
    end
  end

  defp validate([], _index, _until_check, budget), do: DeadlineBudget.ensure_active(budget)

  defp validate([command | commands], index, until_check, budget) do
    with :ok <- PipelineCommand.validate(command, index) do
      validate(commands, index + 1, until_check - 1, budget)
    end
  end
end
