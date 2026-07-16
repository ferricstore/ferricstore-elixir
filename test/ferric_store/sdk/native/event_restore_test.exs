defmodule FerricStore.SDK.Native.EventRestoreTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.EventRestore

  test "restore lifecycle is typed and rejects stale completions" do
    restore = EventRestore.new()
    refute EventRestore.active?(restore)
    assert EventRestore.next_attempt(restore) == 1

    {token, restore} = EventRestore.begin(restore, self())

    assert %EventRestore{status: :inflight, token: ^token, connection: connection, attempt: 1} =
             restore

    assert connection == self()
    refute EventRestore.inflight?(restore, make_ref())
    assert EventRestore.inflight?(restore, token)
    assert EventRestore.next_attempt(restore) == 2

    restore = EventRestore.reset(restore)
    refute EventRestore.active?(restore)
    assert EventRestore.next_attempt(restore) == 1
  end

  test "retry activation preserves the attempt counter without parallel fields" do
    restore = EventRestore.retry(EventRestore.new(), 3, :closed, self(), 0)

    assert %EventRestore{status: :retry_wait, attempt: 3, last_error: :closed, token: token} =
             restore

    assert_receive {:retry_event_restore, ^token}
    assert :stale = EventRestore.activate_retry(restore, make_ref())
    assert {:ok, restore} = EventRestore.activate_retry(restore, token)
    refute EventRestore.active?(restore)
    assert EventRestore.next_attempt(restore) == 4
  end
end
