defmodule FerricStore.SDK.Native.ConnectionRequestTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.ConnectionRequest

  test "stale transport failures cannot tear down the connection" do
    state = empty_state()

    assert {:ok, ^state} =
             ConnectionRequest.complete_encoding(
               state,
               41,
               make_ref(),
               {:transport_error, :closed}
             )
  end

  test "an authorized transport failure still fails the connection" do
    encode_token = make_ref()

    pending = %{
      encode_token: encode_token,
      phase: :sending,
      timer: nil,
      target: :heartbeat
    }

    state = %{empty_state() | pending: %{41 => pending}}

    assert {:stop, {:send_failed, :closed}, next_state} =
             ConnectionRequest.complete_encoding(
               state,
               41,
               encode_token,
               {:transport_error, :closed}
             )

    assert next_state.pending == %{}
  end

  test "encoding authorization stores response context on the matching request" do
    encode_token = make_ref()
    pending = %{encode_token: encode_token, phase: :encoding, response_context: nil}
    state = %{empty_state() | pending: %{41 => pending}}

    assert {:authorize, next_state} =
             ConnectionRequest.encoding_ready(state, 41, encode_token, [:base, :unknown])

    assert next_state.pending[41].phase == :sending
    assert next_state.pending[41].response_context == [:base, :unknown]
  end

  test "rejects a duplicate asynchronous target before it can corrupt the cancellation index" do
    tag = make_ref()
    target = {:message, self(), tag}

    state =
      empty_state()
      |> Map.put(:drain, %{active: false})
      |> Map.put(:pending_targets, %{target => 41})

    assert {:error, :duplicate_request_target, ^state} =
             ConnectionRequest.submit(state, target, 0x0101, %{}, 1, 1_000, :infinity)
  end

  defp empty_state do
    %{
      pending: %{},
      pending_targets: %{},
      pending_lanes: %{},
      data_in_flight: 0,
      response_chunk_bytes: 0,
      response_chunk_frames: 0
    }
  end
end
