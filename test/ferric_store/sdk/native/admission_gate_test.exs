defmodule FerricStore.SDK.Native.AdmissionGateTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.AdmissionGate

  test "extra releases cannot underflow the unsigned admission counter" do
    gate = AdmissionGate.new(1)

    assert :ok = AdmissionGate.release(gate)
    assert :ok = AdmissionGate.release(gate)
    assert AdmissionGate.size(gate) == 0

    assert :ok = AdmissionGate.acquire(gate)
    assert AdmissionGate.size(gate) == 1
    assert {:error, :client_backpressure} = AdmissionGate.acquire(gate)
    assert AdmissionGate.size(gate) == 1

    assert :ok = AdmissionGate.release(gate)
    assert AdmissionGate.size(gate) == 0
  end
end
