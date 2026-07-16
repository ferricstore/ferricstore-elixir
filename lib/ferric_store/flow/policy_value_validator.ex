defmodule FerricStore.Flow.PolicyValueValidator do
  @moduledoc false

  alias FerricStore.DeadlineBudget
  alias FerricStore.Flow.{PolicyIndexValidator, PolicyRetryValidator, PolicyStateValidator}

  @spec validate(map()) :: :ok | {:error, {:invalid_policy_option, binary()}}
  def validate(options) when is_map(options) do
    with :ok <- PolicyIndexValidator.validate(options),
         :ok <- PolicyRetryValidator.validate(Map.get(options, "retry"), "retry"),
         :ok <-
           PolicyRetryValidator.validate_retention(Map.get(options, "retention"), "retention") do
      PolicyStateValidator.validate(Map.get(options, "states"))
    end
  end

  @spec validate(map(), DeadlineBudget.t()) ::
          :ok | {:error, :timeout | {:invalid_policy_option, binary()}}
  def validate(options, %DeadlineBudget{} = budget) when is_map(options) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         :ok <- PolicyIndexValidator.validate(options),
         :ok <- PolicyRetryValidator.validate(Map.get(options, "retry"), "retry"),
         :ok <-
           PolicyRetryValidator.validate_retention(Map.get(options, "retention"), "retention"),
         :ok <- PolicyStateValidator.validate(Map.get(options, "states"), budget) do
      DeadlineBudget.ensure_active(budget)
    end
  end
end
