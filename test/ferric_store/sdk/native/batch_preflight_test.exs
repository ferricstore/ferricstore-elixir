defmodule FerricStore.SDK.Native.BatchPreflightTest do
  use ExUnit.Case, async: true

  alias FerricStore.RequestContext

  alias FerricStore.SDK.Native.{
    BatchOperation,
    BatchPreflight,
    BatchScheduler
  }

  alias FerricStore.SDK.Native.Coordinator.State

  test "synchronous connection failures release preflight concurrency slots" do
    context = RequestContext.new([timeout: :infinity], 5_000)

    batch =
      BatchOperation.new(
        {self(), make_ref()},
        0x0105,
        [:first, :second],
        2,
        & &1,
        & &1,
        context
      )
      |> Map.put(:max_concurrency, 1)

    groups = [group(0, :first), group(1, :second)]
    owner = self()

    ensure_connection = fn state, _endpoint, endpoint_key, _lane_id, _waiter ->
      send(owner, {:connection_attempt, endpoint_key})
      {:error, {:connect_failed, endpoint_key}, state}
    end

    assert {:finish, state, batch_id} =
             BatchPreflight.start(%State{}, batch, groups, ensure_connection)

    assert batch_id == batch.id
    assert_receive {:connection_attempt, :first}
    assert_receive {:connection_attempt, :second}

    completed = BatchScheduler.fetch!(state.batch_scheduler, batch.id)
    assert completed.connections_remaining == 0
    assert completed.connections_inflight == 0
    assert Enum.map(completed.failures, & &1.indexes) |> Enum.sort() == [[0], [1]]
  end

  test "expired preflight does not start connection acquisition" do
    context = RequestContext.new([timeout: 0], 100)

    batch =
      BatchOperation.new(
        {self(), make_ref()},
        0x0105,
        [:first],
        1,
        & &1,
        & &1,
        context
      )

    ensure_connection = fn _state, _endpoint, _key, _lane_id, _waiter ->
      flunk("expired preflight must not acquire a connection")
    end

    assert {:timeout, state, batch_id} =
             BatchPreflight.start(%State{}, batch, [group(0, :first)], ensure_connection)

    assert batch_id == batch.id
    assert BatchScheduler.fetch!(state.batch_scheduler, batch.id).connections_remaining == 1
  end

  defp group(index, endpoint_key) do
    %{
      indexes: [index],
      route: %{
        endpoint: %{host: "127.0.0.1", native_port: 6_379},
        endpoint_key: endpoint_key,
        lane_id: 1
      }
    }
  end
end
