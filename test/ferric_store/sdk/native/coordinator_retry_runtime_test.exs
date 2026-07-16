defmodule FerricStore.SDK.Native.CoordinatorRetryRuntimeTest do
  use ExUnit.Case, async: true

  alias FerricStore.RequestContext

  alias FerricStore.SDK.Native.{
    CoordinatorRequest,
    CoordinatorRetryRuntime,
    Topology,
    TopologyRuntime
  }

  alias FerricStore.SDK.Native.Coordinator.State

  test "ordinary control retries preserve the cached topology connection key" do
    endpoint = %{host: "control.example", native_port: 6_388, node: "control"}
    endpoint_key = Topology.endpoint_key(endpoint)

    state =
      %State{}
      |> TopologyRuntime.put_initial(%Topology{
        endpoints: %{endpoint_key => endpoint},
        control_endpoint: endpoint
      })

    tag = make_ref()

    request =
      CoordinatorRequest.control({self(), make_ref()}, 0x0003, %{}, RequestContext.new([], 100))

    state = State.put_pending_request(state, tag, request)
    test_pid = self()

    callbacks = %{
      default_lane: fn _opcode -> 0 end,
      ensure_connection: fn state, queued_endpoint, connection_key, waiter ->
        send(test_pid, {:queued, queued_endpoint, connection_key, waiter})
        {:waiting, state}
      end
    }

    assert %State{} = CoordinatorRetryRuntime.dispatch_pending(state, tag, callbacks)
    assert_receive {:queued, ^endpoint, ^endpoint_key, ^tag}
  end

  test "control retries still normalize an explicit endpoint override" do
    topology_endpoint = %{host: "control.example", native_port: 6_388, node: "control"}
    endpoint_key = Topology.endpoint_key(topology_endpoint)
    override = %{host: "override.example", native_port: 7_000}

    state =
      %State{}
      |> TopologyRuntime.put_initial(%Topology{
        endpoints: %{endpoint_key => topology_endpoint},
        control_endpoint: topology_endpoint
      })

    tag = make_ref()

    request =
      CoordinatorRequest.control(
        {self(), make_ref()},
        0x0003,
        %{},
        RequestContext.new([endpoint: override], 100)
      )

    state = State.put_pending_request(state, tag, request)
    test_pid = self()

    callbacks = %{
      default_lane: fn _opcode -> 0 end,
      ensure_connection: fn state, queued_endpoint, connection_key, waiter ->
        send(test_pid, {:queued, queued_endpoint, connection_key, waiter})
        {:waiting, state}
      end
    }

    assert %State{} = CoordinatorRetryRuntime.dispatch_pending(state, tag, callbacks)
    assert_receive {:queued, ^override, nil, ^tag}
  end

  test "event retries without an override preserve the cached topology connection key" do
    endpoint = %{host: "control.example", native_port: 6_388, node: "control"}
    endpoint_key = Topology.endpoint_key(endpoint)

    state =
      %State{}
      |> TopologyRuntime.put_initial(%Topology{
        endpoints: %{endpoint_key => endpoint},
        control_endpoint: endpoint
      })

    tag = make_ref()

    request = %{
      kind: :event_subscribe,
      opts: RequestContext.new([], 100),
      opcode: 0x000A,
      attempt: 1
    }

    state = State.put_pending_request(state, tag, request)
    test_pid = self()

    callbacks = %{
      ensure_connection: fn state, queued_endpoint, connection_key, waiter ->
        send(test_pid, {:queued, queued_endpoint, connection_key, waiter})
        {:waiting, state}
      end
    }

    assert %State{} = CoordinatorRetryRuntime.dispatch_pending(state, tag, callbacks)
    assert_receive {:queued, ^endpoint, ^endpoint_key, ^tag}
  end
end
