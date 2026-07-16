defmodule FerricStore.SDK.Native.ConnectionPoolTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.ConnectionPool

  test "checkout owns cached, connecting, and total-cap transitions" do
    pool = ConnectionPool.new(max_connections: 1, max_connecting: 1)
    waiter = make_ref()

    assert {:start, pool} = ConnectionPool.checkout(pool, :primary, waiter)

    attempt = %{waiters: MapSet.new([waiter]), monitor: make_ref()}
    pool = ConnectionPool.put_attempt(pool, :primary, attempt)
    second_waiter = make_ref()

    assert {:waiting, joined} = ConnectionPool.checkout(pool, :primary, second_waiter)
    assert MapSet.member?(ConnectionPool.fetch_attempt!(joined, :primary).waiters, second_waiter)

    assert {:error, :connection_backpressure, ^joined} =
             ConnectionPool.checkout(joined, :secondary, make_ref())

    {_attempt, pool} = ConnectionPool.pop_attempt(joined, :primary)
    connection = spawn(fn -> Process.sleep(:infinity) end)
    {:ok, pool} = ConnectionPool.track(pool, :primary, connection)

    assert {:ready, ^connection, ^pool} = ConnectionPool.checkout(pool, :primary, make_ref())

    assert {:error, :connection_backpressure, ^pool} =
             ConnectionPool.checkout(pool, :secondary, make_ref())

    Process.exit(connection, :kill)
  end

  test "refresh reservations hold capacity across new and replacement connections" do
    pool = ConnectionPool.new(max_connections: 1, max_connecting: 1)

    assert {:ok, :new, reserved} = ConnectionPool.reserve_refresh(pool, false)
    assert ConnectionPool.full?(reserved)

    assert {:error, :connection_backpressure, ^reserved} =
             ConnectionPool.reserve_refresh(reserved, false)

    pool = ConnectionPool.release_refresh(reserved)
    refute ConnectionPool.full?(pool)

    connection = spawn(fn -> Process.sleep(:infinity) end)
    {:ok, pool} = ConnectionPool.track(pool, :primary, connection)

    assert {:ok, :replacement, reserved} = ConnectionPool.reserve_refresh(pool, true)
    pool = ConnectionPool.remove_connection(reserved, connection)
    assert ConnectionPool.full?(pool)

    pool = ConnectionPool.release_refresh(pool)
    refute ConnectionPool.full?(pool)
    Process.exit(connection, :kill)
  end

  test "pruning and process-down removal return the affected connections" do
    first = spawn(fn -> Process.sleep(:infinity) end)
    second = spawn(fn -> Process.sleep(:infinity) end)

    pool = ConnectionPool.new(max_connections: 2, max_connecting: 1)
    {:ok, pool} = ConnectionPool.track(pool, :first, first)
    {:ok, pool} = ConnectionPool.track(pool, :second, second)

    overflow = spawn(fn -> Process.sleep(:infinity) end)

    assert {:error, :connection_backpressure, ^pool} =
             ConnectionPool.track(pool, :overflow, overflow)

    {pool, stale} = ConnectionPool.prune(pool, MapSet.new([:second]))
    assert stale == [first]
    assert ConnectionPool.connections(pool) == %{second: second}
    assert ConnectionPool.retiring?(pool, first)
    assert ConnectionPool.full?(pool)

    assert {:error, :connection_backpressure, ^pool} =
             ConnectionPool.checkout(pool, :replacement, make_ref())

    pool = ConnectionPool.remove_connection(pool, second)
    assert ConnectionPool.connections(pool) == %{}
    refute ConnectionPool.full?(pool)

    {:ok, pool} = ConnectionPool.track(pool, :replacement, overflow)
    assert ConnectionPool.full?(pool)

    pool = ConnectionPool.remove_connection(pool, first)
    refute ConnectionPool.full?(pool)
    refute ConnectionPool.retiring?(pool, first)

    Process.exit(first, :kill)
    Process.exit(second, :kill)
    Process.exit(overflow, :kill)
  end

  test "connection attempts do not duplicate the typed lifecycle monitor index" do
    pool = ConnectionPool.new(max_connections: 1, max_connecting: 1)
    attempt = %{waiters: MapSet.new(), monitor: make_ref()}
    pool = ConnectionPool.put_attempt(pool, :primary, attempt)

    refute Map.has_key?(pool.attempts, :key_by_monitor)
    assert {^attempt, pool} = ConnectionPool.pop_attempt(pool, :primary)
    assert ConnectionPool.connecting_count(pool) == 0
  end

  test "batch waiters are removed through a reverse attempt index" do
    unrelated_count = 2_000
    batch_id = make_ref()

    pool =
      ConnectionPool.new(
        max_connections: unrelated_count + 2,
        max_connecting: unrelated_count + 2
      )

    pool =
      Enum.reduce(1..unrelated_count, pool, fn key, pool ->
        attempt = %{waiters: MapSet.new([make_ref()]), monitor: make_ref()}
        ConnectionPool.put_attempt(pool, key, attempt)
      end)

    retained_waiter = make_ref()

    pool =
      pool
      |> ConnectionPool.put_attempt(:retained, %{
        waiters: MapSet.new([{:batch, batch_id, :first}, retained_waiter]),
        monitor: make_ref()
      })
      |> ConnectionPool.put_attempt(:emptied, %{
        waiters: MapSet.new([{:batch, batch_id, :second}]),
        monitor: make_ref()
      })

    {:reductions, before} = Process.info(self(), :reductions)
    {emptied, pool} = ConnectionPool.remove_batch_waiters(pool, batch_id)
    {:reductions, after_removal} = Process.info(self(), :reductions)

    assert after_removal - before < 20_000
    assert [{:emptied, %{waiters: emptied_waiters}}] = emptied
    assert MapSet.size(emptied_waiters) == 1
    assert ConnectionPool.fetch_attempt!(pool, :retained).waiters == MapSet.new([retained_waiter])
    assert ConnectionPool.fetch_attempt(pool, :emptied) == nil
    assert ConnectionPool.connecting_count(pool) == unrelated_count + 1
    assert pool.attempts.waiters_by_batch == %{}
  end

  test "sequential batch waiter removal grows linearly" do
    small = batch_waiter_removal_reductions(250)
    large = batch_waiter_removal_reductions(500)

    assert large < small * 3,
           "expected linear waiter removal, got #{small} reductions for 250 waiters and #{large} for 500"
  end

  test "connections are removed by process through a reverse index" do
    count = 500
    connections = Enum.map(1..count, fn _index -> spawn(fn -> Process.sleep(:infinity) end) end)

    on_exit(fn ->
      Enum.each(connections, fn connection ->
        if Process.alive?(connection), do: Process.exit(connection, :kill)
      end)
    end)

    pool = ConnectionPool.new(max_connections: count, max_connecting: 1)

    pool =
      connections
      |> Enum.with_index()
      |> Enum.reduce(pool, fn {connection, key}, pool ->
        {:ok, pool} = ConnectionPool.track(pool, key, connection)
        pool
      end)

    {:reductions, before} = Process.info(self(), :reductions)
    pool = Enum.reduce(connections, pool, &ConnectionPool.remove_connection(&2, &1))
    {:reductions, after_removal} = Process.info(self(), :reductions)

    assert after_removal - before < 500_000
    assert ConnectionPool.connections(pool) == %{}
    assert pool.connection_keys_by_pid == %{}
  end

  test "cached checkout cost grows sublinearly with sessions per endpoint" do
    {small_pool, small_connections} = endpoint_pool(8)
    {large_pool, large_connections} = endpoint_pool(256)
    connections = small_connections ++ large_connections

    on_exit(fn ->
      Enum.each(connections, fn connection ->
        if Process.alive?(connection), do: Process.exit(connection, :kill)
      end)
    end)

    {small_reductions, _small_pool} = checkout_reductions(small_pool, 100)
    {large_reductions, _large_pool} = checkout_reductions(large_pool, 100)

    assert large_reductions < small_reductions * 4
  end

  test "slot availability does not allocate per endpoint session" do
    {small_pool, small_connections} = endpoint_pool(8)
    {large_pool, large_connections} = endpoint_pool(256)
    connections = small_connections ++ large_connections

    on_exit(fn ->
      Enum.each(connections, fn connection ->
        if Process.alive?(connection), do: Process.exit(connection, :kill)
      end)
    end)

    small_gcs = slot_availability_minor_gcs(small_pool, 20_000)
    large_gcs = slot_availability_minor_gcs(large_pool, 20_000)

    assert large_gcs <= small_gcs * 2 + 10,
           "slot availability allocated with session count: #{small_gcs} versus #{large_gcs} minor GCs"
  end

  test "equally loaded sessions retain round-robin checkout order" do
    {pool, connections} = endpoint_pool(3)

    on_exit(fn ->
      Enum.each(connections, fn connection ->
        if Process.alive?(connection), do: Process.exit(connection, :kill)
      end)
    end)

    {selected, _pool} =
      Enum.map_reduce(1..6, pool, fn _index, pool ->
        {:ready, connection, pool} = ConnectionPool.checkout(pool, :primary, make_ref())
        {connection, pool}
      end)

    assert selected == connections ++ connections
  end

  test "a busy endpoint expands to its configured session count and selects the idle session" do
    first = spawn(fn -> Process.sleep(:infinity) end)
    second = spawn(fn -> Process.sleep(:infinity) end)

    on_exit(fn ->
      Enum.each([first, second], fn connection ->
        if Process.alive?(connection), do: Process.exit(connection, :kill)
      end)
    end)

    pool =
      ConnectionPool.new(
        max_connections: 2,
        max_connecting: 1,
        connections_per_endpoint: 2
      )

    {:ok, pool} = ConnectionPool.track(pool, :primary, first)

    pool = ConnectionPool.mark_busy(pool, first)
    assert {:start, pool} = ConnectionPool.checkout(pool, :primary, make_ref())

    attempt = %{waiters: MapSet.new(), monitor: make_ref()}
    pool = ConnectionPool.put_attempt(pool, :primary, attempt)

    assert {:waiting, pool} = ConnectionPool.checkout(pool, :primary, make_ref())

    {_attempt, pool} = ConnectionPool.pop_attempt(pool, :primary)
    {:ok, pool} = ConnectionPool.track(pool, :primary, second)

    assert {:ready, ^second, pool} = ConnectionPool.checkout(pool, :primary, make_ref())

    assert MapSet.new(ConnectionPool.connection_values(pool)) == MapSet.new([first, second])
  end

  test "capacity checkout reserves negotiated connection and lane windows" do
    first = spawn(fn -> Process.sleep(:infinity) end)
    second = spawn(fn -> Process.sleep(:infinity) end)

    on_exit(fn ->
      Enum.each([first, second], fn connection ->
        if Process.alive?(connection), do: Process.exit(connection, :kill)
      end)
    end)

    limits = %{max_in_flight: 1, max_in_flight_per_lane: 1}

    pool =
      ConnectionPool.new(
        max_connections: 2,
        max_connecting: 1,
        connections_per_endpoint: 2
      )

    {:ok, pool} = ConnectionPool.track(pool, :primary, first, limits)
    assert {:ready, ^first, pool} = ConnectionPool.checkout_capacity(pool, :primary, 1, :first)
    assert {:start, ^pool} = ConnectionPool.checkout_capacity(pool, :primary, 2, :second)

    {:ok, pool} = ConnectionPool.track(pool, :primary, second, limits)
    assert {:ready, ^second, pool} = ConnectionPool.checkout_capacity(pool, :primary, 2, :second)
    assert {:capacity, ^pool} = ConnectionPool.checkout_capacity(pool, :primary, 3, :third)

    pool = ConnectionPool.mark_idle(pool, first, 1)
    assert {:ready, ^first, _pool} = ConnectionPool.checkout_capacity(pool, :primary, 3, :third)
  end

  defp endpoint_pool(count) do
    connections =
      Enum.map(1..count, fn _index -> spawn(fn -> Process.sleep(:infinity) end) end)

    pool =
      ConnectionPool.new(
        max_connections: count,
        max_connecting: 1,
        connections_per_endpoint: count
      )

    pool =
      Enum.reduce(connections, pool, fn connection, pool ->
        {:ok, pool} = ConnectionPool.track(pool, :primary, connection)
        pool
      end)

    {pool, connections}
  end

  defp checkout_reductions(pool, count) do
    {:reductions, before_checkout} = Process.info(self(), :reductions)
    pool = run_checkouts(pool, count)
    {:reductions, after_checkout} = Process.info(self(), :reductions)
    {after_checkout - before_checkout, pool}
  end

  defp run_checkouts(pool, 0), do: pool

  defp run_checkouts(pool, remaining) do
    {:ready, _connection, pool} = ConnectionPool.checkout(pool, :primary, make_ref())
    run_checkouts(pool, remaining - 1)
  end

  defp batch_waiter_removal_reductions(count) do
    batch_ids = Enum.map(1..count, fn _index -> make_ref() end)

    waiters =
      batch_ids
      |> Enum.with_index()
      |> MapSet.new(fn {batch_id, index} -> {:batch, batch_id, index} end)

    pool = ConnectionPool.new(max_connections: 1, max_connecting: 1)
    pool = ConnectionPool.put_attempt(pool, :primary, %{waiters: waiters, monitor: make_ref()})
    :erlang.garbage_collect(self())
    {:reductions, before_removal} = Process.info(self(), :reductions)

    pool =
      Enum.reduce(batch_ids, pool, fn batch_id, pool ->
        {_emptied, pool} = ConnectionPool.remove_batch_waiters(pool, batch_id)
        pool
      end)

    {:reductions, after_removal} = Process.info(self(), :reductions)
    assert ConnectionPool.connecting_count(pool) == 0
    after_removal - before_removal
  end

  defp slot_availability_minor_gcs(pool, iterations) do
    :erlang.garbage_collect(self())
    before_gcs = minor_gcs()
    run_slot_checks(pool, iterations)
    minor_gcs() - before_gcs
  end

  defp run_slot_checks(_pool, 0), do: :ok

  defp run_slot_checks(pool, remaining) do
    assert ConnectionPool.slot_available?(pool, :primary)
    run_slot_checks(pool, remaining - 1)
  end

  defp minor_gcs do
    self()
    |> Process.info(:garbage_collection)
    |> elem(1)
    |> Keyword.fetch!(:minor_gcs)
  end
end
