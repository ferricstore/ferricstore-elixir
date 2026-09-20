defmodule FerricStore.IntegrationGateTest do
  use ExUnit.Case, async: true

  alias FerricStore.Test.IntegrationGate

  test "skips only the rewind persistence regression for the known 0.11.4 floor" do
    assert is_binary(IntegrationGate.rewind_reason_skip("0.11.4"))
    assert IntegrationGate.rewind_reason_skip("0.11.20") == false
  end

  test "unknown or unset server versions fail closed by running every tagged test" do
    assert IntegrationGate.rewind_reason_skip(nil) == false
    assert IntegrationGate.rewind_reason_skip("") == false
    assert IntegrationGate.rewind_reason_skip("latest") == false
  end

  test "a specific skip survives integration include filters without skipping the baseline test" do
    reason = IntegrationGate.rewind_reason_skip("0.11.4")

    assert ExUnit.Filters.eval(
             [integration: true],
             [],
             %{integration: true, requires_ferricstore_0_11_19: true, skip: reason},
             %{}
           ) == {:skipped, reason}

    assert ExUnit.Filters.eval(
             [integration: true],
             [],
             %{integration: true, skip: false},
             %{}
           ) == :ok
  end
end
