defmodule FerricStore.Queue.BatchRunner do
  @moduledoc false

  alias FerricStore.OptionList

  @max_concurrency 256
  @max_options 65

  @spec max_concurrency!(keyword()) :: pos_integer()
  def max_concurrency!(opts) do
    case OptionList.validate(opts, @max_options) do
      :ok -> concurrency_value!(Keyword.get(opts, :max_concurrency))
      {:error, {:options, {:too_many_options, _details}}} -> too_many_options!()
      {:error, {:options, {:duplicate_options, duplicates}}} -> duplicate_options!(duplicates)
      {:error, {:options, _invalid}} -> invalid_options!()
    end
  end

  @spec map(list(), (term() -> term()), (term(), term() -> term()), pos_integer()) :: list()
  def map([], _settle_job, _settle_task_exit, _max_concurrency), do: []

  def map(jobs, settle_job, settle_task_exit, max_concurrency) do
    {:ok, supervisor} = Task.Supervisor.start_link()

    try do
      results =
        Task.Supervisor.async_stream_nolink(supervisor, jobs, settle_job,
          max_concurrency: max_concurrency,
          ordered: true,
          timeout: :infinity
        )

      Enum.zip_with(jobs, results, fn
        _job, {:ok, result} -> result
        job, {:exit, reason} -> settle_task_exit.(job, reason)
      end)
    after
      Supervisor.stop(supervisor)
    end
  end

  defp concurrency_value!(nil), do: System.schedulers_online() |> max(1) |> min(@max_concurrency)

  defp concurrency_value!(value)
       when is_integer(value) and value > 0 and value <= @max_concurrency,
       do: value

  defp concurrency_value!(value) when not is_integer(value) or value <= 0,
    do: raise(ArgumentError, "max_concurrency must be a positive integer")

  defp concurrency_value!(_value),
    do: raise(ArgumentError, "max_concurrency must be between 1 and #{@max_concurrency}")

  defp too_many_options!,
    do: raise(ArgumentError, "queue options exceed #{@max_options} entries")

  defp duplicate_options!(duplicates),
    do: raise(ArgumentError, "duplicate queue options: #{inspect(duplicates)}")

  defp invalid_options!, do: raise(ArgumentError, "queue options must be a keyword list")
end
