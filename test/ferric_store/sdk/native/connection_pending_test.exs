defmodule FerricStore.SDK.Native.ConnectionPendingTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.ConnectionPending

  test "dropping an older request cannot erase a newer target index entry" do
    target = {:message, self(), make_ref()}
    older = pending(target)
    newer = pending(target)

    state = %{
      pending: %{10 => older, 11 => newer},
      pending_targets: %{target => 11},
      pending_lanes: %{1 => 2},
      data_in_flight: 2,
      response_chunk_bytes: 0,
      response_chunk_frames: 0
    }

    state = ConnectionPending.drop(state, 10, older)

    assert state.pending_targets == %{target => 11}
    assert state.pending == %{11 => newer}
  end

  defp pending(target) do
    %{
      target: target,
      lane_id: 1,
      flow_controlled?: true,
      chunk_bytes: 0,
      chunk_frames: 0
    }
  end
end
