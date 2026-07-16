defmodule FerricStore.Flow.ResponseResultList do
  @moduledoc false

  alias FerricStore.DeadlineBudget
  @deadline_check_interval 256

  @spec map(list(), DeadlineBudget.t(), (term() -> {:ok, term()} | {:error, term()})) ::
          {:ok, list()} | {:error, term()}
  def map(items, %DeadlineBudget{} = budget, mapper) when is_function(mapper, 1) do
    do_map(items, [], 0, budget, mapper)
  end

  defp do_map(items, mapped, 0, budget, mapper) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         do: do_map(items, mapped, @deadline_check_interval, budget, mapper)
  end

  defp do_map([], mapped, _until_check, budget, _mapper) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         mapped = Enum.reverse(mapped),
         :ok <- DeadlineBudget.ensure_active(budget),
         do: {:ok, mapped}
  end

  defp do_map([item | items], mapped, until_check, budget, mapper) do
    case mapper.(item) do
      {:ok, value} -> do_map(items, [value | mapped], until_check - 1, budget, mapper)
      {:error, _reason} = error -> error
    end
  end

  defp do_map(_improper, _mapped, _until_check, _budget, _mapper),
    do: {:error, :improper_list}
end
