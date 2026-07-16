defmodule FerricStore.SDK.Native.CoordinatorSubmissionRuntimeTest do
  use ExUnit.Case, async: true

  alias FerricStore.RequestContext

  alias FerricStore.SDK.Native.{
    CoordinatorSubmissionRuntime,
    Topology,
    TopologyRuntime
  }

  alias FerricStore.SDK.Native.Coordinator.State

  test "ordinary control requests preserve the cached topology connection key" do
    endpoint = %{host: "control.example", native_port: 6_388, node: "control"}
    endpoint_key = Topology.endpoint_key(endpoint)

    state =
      %State{}
      |> TopologyRuntime.put_initial(%Topology{
        endpoints: %{endpoint_key => endpoint},
        control_endpoint: endpoint
      })

    test_pid = self()

    callbacks = %{
      queue: fn state, queued_endpoint, lane_id, request, connection_key ->
        send(
          test_pid,
          {:queued, queued_endpoint, lane_id, request.kind, connection_key}
        )

        {:noreply, state}
      end
    }

    context = RequestContext.new([], 100)

    assert {:noreply, ^state} =
             CoordinatorSubmissionRuntime.control(
               state,
               {self(), make_ref()},
               0x0003,
               %{},
               context,
               callbacks
             )

    assert_receive {:queued, ^endpoint, 0, :control, ^endpoint_key}
  end
end
