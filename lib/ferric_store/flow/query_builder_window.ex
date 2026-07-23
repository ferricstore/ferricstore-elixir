defmodule FerricStore.Flow.QueryBuilderWindow do
  @moduledoc false

  @max_time 9_007_199_254_740_991

  def add(builder, opts) do
    case bounds(Keyword.get(opts, :from_ms), Keyword.get(opts, :to_ms)) do
      :empty ->
        {:ok, builder}

      {:ok, from_ms, to_ms} ->
        {:ok,
         %{
           builder
           | predicates: [
               "updated_at_ms BETWEEN @from_ms AND @to_ms" | builder.predicates
             ],
             params:
               builder.params
               |> Map.put("from_ms", from_ms)
               |> Map.put("to_ms", to_ms)
         }}

      :error ->
        {:error, {:invalid_flow_query_option, :time_window}}
    end
  end

  defp bounds(nil, nil), do: :empty

  defp bounds(from_ms, to_ms) do
    lower = from_ms || 0
    upper = to_ms || @max_time

    if valid?(lower) and valid?(upper) and lower <= upper,
      do: {:ok, lower, upper},
      else: :error
  end

  defp valid?(value), do: is_integer(value) and value in 0..@max_time
end
