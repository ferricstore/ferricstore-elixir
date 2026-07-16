defmodule FerricStore.SDK.Native.ConnectionAttemptsTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.ConnectionAttempts

  test "randomized operations preserve waiter indexes and removal semantics" do
    :rand.seed(:exsss, {17, 23, 41})
    batch_ids = Enum.map(1..8, fn _index -> make_ref() end)
    waiters = waiter_pool(batch_ids)
    keys = Enum.map(1..20, &{:endpoint, &1})

    {attempts, model} =
      Enum.reduce(1..20_000, {%ConnectionAttempts{}, %{}}, fn _step, state ->
        state
        |> apply_random_operation(keys, waiters, batch_ids)
        |> assert_consistent()
      end)

    assert attempts.by_key == model
  end

  defp apply_random_operation({attempts, model}, keys, waiters, batch_ids) do
    key = random(keys)
    waiter = random(waiters)

    case :rand.uniform(5) do
      1 -> put_attempt(attempts, model, key, waiters)
      2 -> pop_attempt(attempts, model, key)
      3 -> add_waiter(attempts, model, key, waiter)
      4 -> remove_waiter(attempts, model, key, waiter)
      5 -> remove_batch(attempts, model, random(batch_ids))
    end
  end

  defp put_attempt(attempts, model, key, waiters) do
    selected = waiters |> Enum.take_random(:rand.uniform(8) - 1) |> MapSet.new()
    attempt = %{waiters: selected, token: make_ref()}
    {ConnectionAttempts.put(attempts, key, attempt), Map.put(model, key, attempt)}
  end

  defp pop_attempt(attempts, model, key) do
    expected = Map.get(model, key)
    assert {^expected, attempts} = ConnectionAttempts.pop(attempts, key)
    {attempts, Map.delete(model, key)}
  end

  defp add_waiter(attempts, model, key, waiter) do
    model = Map.update(model, key, nil, &%{&1 | waiters: MapSet.put(&1.waiters, waiter)})
    model = if is_nil(model[key]), do: Map.delete(model, key), else: model
    {ConnectionAttempts.add_waiter(attempts, key, waiter), model}
  end

  defp remove_waiter(attempts, model, key, waiter) do
    case Map.get(model, key) do
      nil ->
        assert {:missing, attempts} = ConnectionAttempts.remove_waiter(attempts, key, waiter)
        {attempts, model}

      %{waiters: waiters} = attempt ->
        remove_modeled_waiter(attempts, model, key, waiter, attempt, waiters)
    end
  end

  defp remove_modeled_waiter(attempts, model, key, waiter, attempt, waiters) do
    if MapSet.member?(waiters, waiter) do
      remaining = MapSet.delete(waiters, waiter)

      if MapSet.size(remaining) == 0 do
        assert {:empty, ^attempt, attempts} =
                 ConnectionAttempts.remove_waiter(attempts, key, waiter)

        {attempts, Map.delete(model, key)}
      else
        assert {:remaining, attempts} = ConnectionAttempts.remove_waiter(attempts, key, waiter)
        {attempts, Map.put(model, key, %{attempt | waiters: remaining})}
      end
    else
      assert {:missing, attempts} = ConnectionAttempts.remove_waiter(attempts, key, waiter)
      {attempts, model}
    end
  end

  defp remove_batch(attempts, model, batch_id) do
    {expected_emptied, model} =
      Enum.reduce(model, {%{}, model}, fn {key, attempt}, {emptied, model} ->
        remaining = Enum.reject(attempt.waiters, &batch_waiter?(&1, batch_id)) |> MapSet.new()

        cond do
          remaining == attempt.waiters -> {emptied, model}
          MapSet.size(remaining) == 0 -> {Map.put(emptied, key, attempt), Map.delete(model, key)}
          true -> {emptied, Map.put(model, key, %{attempt | waiters: remaining})}
        end
      end)

    {emptied, attempts} = ConnectionAttempts.remove_batch_waiters(attempts, batch_id)
    assert Map.new(emptied) == expected_emptied
    {attempts, model}
  end

  defp assert_consistent({attempts, model} = state) do
    assert attempts.by_key == model
    assert attempts.waiters_by_batch == expected_batch_index(model)
    state
  end

  defp expected_batch_index(model) do
    Enum.reduce(model, %{}, fn {key, attempt}, index ->
      Enum.reduce(attempt.waiters, index, &index_batch_waiter(&1, &2, key))
    end)
  end

  defp index_batch_waiter({:batch, batch_id, _group} = waiter, index, key) do
    Map.update(index, batch_id, %{key => MapSet.new([waiter])}, fn by_key ->
      Map.update(by_key, key, MapSet.new([waiter]), &MapSet.put(&1, waiter))
    end)
  end

  defp index_batch_waiter(_other, index, _key), do: index

  defp batch_waiter?({:batch, batch_id, _group}, batch_id), do: true
  defp batch_waiter?(_waiter, _batch_id), do: false

  defp waiter_pool(batch_ids) do
    batch_waiters =
      for batch_id <- batch_ids, group <- 1..4, do: {:batch, batch_id, {:group, group}}

    batch_waiters ++ Enum.map(1..16, &{:request, &1})
  end

  defp random(values), do: Enum.at(values, :rand.uniform(length(values)) - 1)
end
