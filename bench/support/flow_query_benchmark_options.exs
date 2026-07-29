defmodule FerricStore.FlowQueryBenchmarkOptions do
  @moduledoc false

  @budget_options [
    :max_full_decode_reductions,
    :max_projected_decode_reductions,
    :max_count_decode_reductions,
    :max_raw_validation_reductions
  ]

  def parse!(args) do
    {opts, rest, invalid} =
      OptionParser.parse(args,
        strict: [
          iterations: :integer,
          records: :integer,
          heap_samples: :integer,
          max_full_decode_reductions: :integer,
          max_projected_decode_reductions: :integer,
          max_count_decode_reductions: :integer,
          max_raw_validation_reductions: :integer
        ]
      )

    unless rest == [] and invalid == [],
      do: raise(ArgumentError, "unknown benchmark arguments: #{inspect(rest ++ invalid)}")

    opts
    |> Keyword.put_new(:iterations, 1_000)
    |> Keyword.put_new(:records, 100)
    |> Keyword.put_new(:heap_samples, 50)
    |> validate!()
  end

  defp validate!(opts) do
    valid_budgets? =
      Enum.all?(@budget_options, fn option ->
        is_nil(opts[option]) or opts[option] > 0
      end)

    unless opts[:iterations] > 0 and opts[:records] in 1..100 and opts[:heap_samples] >= 0 and
             valid_budgets? do
      raise ArgumentError,
            "iterations and budgets must be positive, records must be in 1..100, and heap samples cannot be negative"
    end

    opts
  end
end
