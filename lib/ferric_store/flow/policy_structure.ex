defmodule FerricStore.Flow.PolicyStructure do
  @moduledoc false

  alias FerricStore.DeadlineBudget
  alias FerricStore.Flow.{PolicyOptionStructure, PolicyStateStructure}

  @set_options ~w(indexed_state_meta indexed_attributes max_active_ms retry retention states)

  def set_options(opts) do
    with {:ok, options} <- PolicyOptionStructure.option_map(opts),
         :ok <- PolicyOptionStructure.validate_options(options, @set_options),
         :ok <- PolicyOptionStructure.validate_retry(Map.get(options, "retry")),
         :ok <- PolicyOptionStructure.validate_retention(Map.get(options, "retention")),
         :ok <- PolicyStateStructure.validate(Map.get(options, "states")) do
      {:ok, options}
    end
  end

  def set_options(opts, %DeadlineBudget{} = budget) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         {:ok, options} <- PolicyOptionStructure.option_map(opts),
         :ok <- PolicyOptionStructure.validate_options(options, @set_options),
         :ok <- PolicyOptionStructure.validate_retry(Map.get(options, "retry")),
         :ok <- PolicyOptionStructure.validate_retention(Map.get(options, "retention")),
         :ok <- PolicyStateStructure.validate(Map.get(options, "states"), budget),
         :ok <- DeadlineBudget.ensure_active(budget) do
      {:ok, options}
    end
  end

  def get_options(opts) do
    with {:ok, options} <- PolicyOptionStructure.option_map(opts),
         :ok <- PolicyOptionStructure.validate_options(options, ["state"]),
         do: {:ok, options}
  end

  def get_options(opts, %DeadlineBudget{} = budget) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         {:ok, options} <- get_options(opts),
         :ok <- DeadlineBudget.ensure_active(budget),
         do: {:ok, options}
  end
end
