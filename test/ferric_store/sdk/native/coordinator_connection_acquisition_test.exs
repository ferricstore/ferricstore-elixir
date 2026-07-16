defmodule FerricStore.SDK.Native.CoordinatorConnectionAcquisitionTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.{
    ConnectionPool,
    CoordinatorConnectionAcquisition,
    EndpointTrust
  }

  alias FerricStore.SDK.Native.Coordinator.State

  test "connection worker startup exits become request errors" do
    operation_supervisor = spawn(fn -> receive do: (:stop -> :ok) end)
    monitor = Process.monitor(operation_supervisor)
    send(operation_supervisor, :stop)
    assert_receive {:DOWN, ^monitor, :process, ^operation_supervisor, :normal}

    state = %State{
      operation_supervisor: operation_supervisor,
      connection_supervisor: self(),
      endpoint_policy: :any,
      endpoint_trust: %EndpointTrust{},
      tls: false
    }

    assert {:error, {:connect_failed, {:exit, _reason}}, next_state} =
             CoordinatorConnectionAcquisition.ensure(
               state,
               %{host: "127.0.0.1", native_port: 6_379},
               nil,
               make_ref()
             )

    assert ConnectionPool.connecting_count(next_state.connection_pool) == 0
  end
end
