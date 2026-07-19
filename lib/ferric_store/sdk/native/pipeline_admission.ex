defmodule FerricStore.SDK.Native.PipelineAdmission do
  @moduledoc false

  alias FerricStore.{BoundedList, DeadlineBudget}
  alias FerricStore.Protocol.PipelineCommand

  @deadline_check_interval 256

  @spec admit(list(), non_neg_integer(), DeadlineBudget.t()) ::
          {:ok, non_neg_integer(), boolean()} | {:error, term()}
  def admit(commands, limit, %DeadlineBudget{} = budget)
      when is_list(commands) and is_integer(limit) and limit >= 0 do
    with {:ok, count} <- count(commands, limit, budget),
         {:ok, generation_cas?} <- validate(commands, 0, 0, budget, false) do
      {:ok, count, generation_cas?}
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

  defp validate(commands, index, 0, budget, generation_cas?) do
    with :ok <- DeadlineBudget.ensure_active(budget) do
      validate(commands, index, @deadline_check_interval, budget, generation_cas?)
    end
  end

  defp validate([], _index, _until_check, budget, generation_cas?) do
    with :ok <- DeadlineBudget.ensure_active(budget), do: {:ok, generation_cas?}
  end

  defp validate([command | commands], index, until_check, budget, generation_cas?) do
    with {:ok, command_generation_cas?} <- PipelineCommand.validate(command, index) do
      validate(
        commands,
        index + 1,
        until_check - 1,
        budget,
        generation_cas? or command_generation_cas?
      )
    end
  end
end
