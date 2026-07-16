defmodule FerricStore.SDK.KVBatchResultsModelTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.KV.BatchResults

  test "randomized grouped KV results reconstruct the submitted index model" do
    :rand.seed(:exsss, {811, 812, 813})

    Enum.each(1..2_000, fn _iteration ->
      count = :rand.uniform(201) - 1

      values =
        List.to_tuple(
          Enum.map(0..max(count - 1, 0), fn index ->
            if count > 0 and :rand.uniform(5) > 1, do: "value-#{index}", else: nil
          end)
          |> Enum.take(count)
        )

      groups = grouped_indexes(count)

      mget_groups =
        Enum.map(groups, fn indexes ->
          %{indexes: indexes, value: Enum.map(indexes, &elem(values, &1))}
        end)

      del_groups =
        Enum.map(groups, fn indexes ->
          %{indexes: indexes, value: :rand.uniform(length(indexes) + 1) - 1}
        end)

      mset_groups = Enum.map(groups, &%{indexes: &1, value: "OK"})

      assert {:ok, Tuple.to_list(values)} == BatchResults.mget(mget_groups, count)

      assert {:ok, Enum.sum(Enum.map(del_groups, & &1.value))} ==
               BatchResults.del(del_groups, count)

      assert {:ok, :ok} == BatchResults.mset(mset_groups, count)
    end)
  end

  defp grouped_indexes(0), do: []

  defp grouped_indexes(count) do
    0..(count - 1)
    |> Enum.shuffle()
    |> random_groups([])
    |> Enum.shuffle()
  end

  defp random_groups([], groups), do: Enum.reverse(groups)

  defp random_groups(indexes, groups) do
    size = min(length(indexes), :rand.uniform(16))
    {group, indexes} = Enum.split(indexes, size)
    random_groups(indexes, [group | groups])
  end
end
