defmodule FerricStore.DeadlineBudgetTest do
  use ExUnit.Case, async: true

  alias FerricStore.DeadlineBudget

  test "a sliced budget never extends its parent's absolute deadline" do
    parent = DeadlineBudget.new(0)
    Process.sleep(2)
    child = DeadlineBudget.slice(parent, 1)

    assert child.expires_at <= parent.expires_at
    assert DeadlineBudget.remaining(child) == 0
  end
end
