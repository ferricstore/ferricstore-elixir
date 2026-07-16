defmodule FerricStore.SDK.Native.PreparationReservationsTest do
  use ExUnit.Case, async: true

  alias FerricStore.RequestContext

  alias FerricStore.SDK.Native.{
    Admission,
    Coordinator,
    PreparationReservations
  }

  alias FerricStore.SDK.Native.Coordinator.State

  test "a preparation reservation holds admission until its caller exits" do
    owner = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> if Process.alive?(owner), do: Process.exit(owner, :kill) end)

    context = RequestContext.new([timeout: 5_000], 5_000)
    from = {owner, make_ref()}
    state = %State{limits: %{pending_requests: 1, batch_items: 100_000, event_subscribers: 1}}

    assert {:reply, {:ok, reservation}, reserved_state} =
             Coordinator.handle_call({:kv_preparation_admission, 1_000, context}, from, state)

    assert is_reference(reservation)
    assert PreparationReservations.size(reserved_state.preparation_reservations) == 1
    assert Admission.full?(reserved_state)

    assert {:reply, {:error, :client_backpressure}, ^reserved_state} =
             Coordinator.handle_call(
               {:kv_preparation_admission, 1_000, context},
               {self(), make_ref()},
               reserved_state
             )

    entry = PreparationReservations.fetch!(reserved_state.preparation_reservations, reservation)
    Process.exit(owner, :kill)
    assert_receive {:DOWN, monitor, :process, ^owner, reason}
    assert monitor == entry.monitor

    assert {:noreply, released_state} =
             Coordinator.handle_info(
               {:DOWN, monitor, :process, owner, reason},
               reserved_state
             )

    assert PreparationReservations.size(released_state.preparation_reservations) == 0
    refute Admission.full?(released_state)
  end
end
