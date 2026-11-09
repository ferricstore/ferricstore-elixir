defmodule FerricStore.Flow.ChildGroupReader do
  @moduledoc false

  # Reads one child group from a parent record. Until fill_values/2 runs, each
  # child's result and error hold {:ref, ref} placeholders for the value read.

  alias FerricStore.Flow.{ChildGroupResults, ChildResult}

  @statuses ~w(running completed failed cancelled)
  @waits ~w(all any none)

  def read(record, group_id) when is_map(record) do
    case get_in(record, ["child_groups", group_id]) do
      nil -> {:ok, nil, []}
      group -> group_results(group, group_id)
    end
  end

  def read(_record, _group_id), do: {:ok, nil, []}

  def fill_values(%ChildGroupResults{children: children} = group, values) do
    {filled, []} =
      Enum.map_reduce(children, values, fn child, remaining ->
        {result, remaining} = take_value(child.result, remaining)
        {error, remaining} = take_value(child.error, remaining)
        {%{child | result: result, error: error}, remaining}
      end)

    %{group | children: filled}
  end

  defp group_results(group, group_id) do
    with {:ok, wait} <- known(group["wait"], @waits, :invalid_child_group_wait),
         {:ok, children} <- children(group) do
      results = %ChildGroupResults{
        group_id: group_id,
        wait: wait,
        resolved: group["resolved"],
        summary: summary(group["summary"] || %{}),
        children: children
      }

      refs =
        Enum.flat_map(children, &Enum.reject([&1.result, &1.error], fn ref -> ref == nil end))

      {:ok, results, Enum.map(refs, fn {:ref, ref} -> ref end)}
    end
  end

  # Until fill_values/2 runs, result and error hold {:ref, ref} placeholders.
  defp children(group) do
    partitions = group["child_partitions"] || %{}
    stored = group["results"] || %{}

    (group["children"] || %{})
    |> Enum.sort_by(fn {id, _status} -> id end)
    |> Enum.reduce_while({:ok, []}, fn {id, status}, {:ok, acc} ->
      case known(status, @statuses, :invalid_child_status) do
        {:ok, status} ->
          entry = Map.get(stored, id) || %{}

          child = %ChildResult{
            id: id,
            partition_key: Map.get(partitions, id),
            status: status,
            result: ref(entry["result_ref"]),
            error: ref(entry["error_ref"])
          }

          {:cont, {:ok, [child | acc]}}

        error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, children} -> {:ok, Enum.reverse(children)}
      error -> error
    end
  end

  defp ref(nil), do: nil
  defp ref(ref), do: {:ref, ref}

  defp take_value({:ref, _ref}, [value | rest]), do: {value, rest}
  defp take_value(nil, values), do: {nil, values}

  defp summary(counts) do
    %{
      total: Map.get(counts, "total", 0),
      completed: Map.get(counts, "completed", 0),
      failed: Map.get(counts, "failed", 0),
      cancelled: Map.get(counts, "cancelled", 0)
    }
  end

  defp known(value, allowed, reason) do
    if value in allowed, do: {:ok, value}, else: {:error, {reason, value}}
  end
end
