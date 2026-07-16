defmodule FerricStore.SessionPolicyTest do
  use ExUnit.Case, async: true

  alias FerricStore.Transport.SessionPolicy

  test "shared session policy classifies draining server frames" do
    assert SessionPolicy.server_frame_action(0x000A) == :drain
    assert SessionPolicy.server_frame_action(0x0010) == :continue
  end

  test "shared session policy owns finite data deadlines" do
    before_ms = System.system_time(:millisecond)
    payload = SessionPolicy.put_deadline(%{"key" => "k"}, 0x0101, 250)
    after_ms = System.system_time(:millisecond)

    assert payload["deadline_ms"] >= before_ms + 250
    assert payload["deadline_ms"] <= after_ms + 250

    caller_deadline = after_ms + 60_000

    overwritten =
      SessionPolicy.put_deadline(
        %{"deadline_ms" => caller_deadline, deadline_ms: caller_deadline},
        0x0101,
        250
      )

    assert %{"deadline_ms" => owned_deadline} = overwritten
    refute Map.has_key?(overwritten, :deadline_ms)
    assert owned_deadline < caller_deadline
    assert owned_deadline >= after_ms + 250

    pipeline = SessionPolicy.put_deadline(%{deadline_ms: 11}, 0x000E, 250)
    assert is_integer(pipeline["deadline_ms"])
    refute Map.has_key?(pipeline, :deadline_ms)
    assert SessionPolicy.put_deadline(%{}, 0x0003, 250) == %{}
    assert SessionPolicy.put_deadline(%{}, 0x0101, :infinity) == %{}
  end

  test "shared session policy owns request-id rollover" do
    assert SessionPolicy.next_request_id(0) == 1
    assert SessionPolicy.next_request_id(0xFFFF_FFFF_FFFF_FFFF) == 1
  end
end
