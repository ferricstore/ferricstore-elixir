defmodule FerricStore.SDK.Native.TopologyRefresherTest do
  use ExUnit.Case, async: false

  alias FerricStore.SDK.Native.{Topology, TopologyRefresher}
  alias FerricStore.Test.NativeServer

  test "a connection exit during bootstrap does not skip healthy fallback candidates" do
    {:ok, failing_server} =
      NativeServer.start_link(
        owner: self(),
        response_fun: fn
          %{opcode: 0x0007} -> :close
          %{opcode: opcode} when opcode in [0x0001, 0x000C] -> NativeServer.startup_payload()
          _request -> "OK"
        end
      )

    {:ok, healthy_server} = NativeServer.start_link(owner: self())
    {:ok, connection_supervisor} = DynamicSupervisor.start_link(strategy: :one_for_one)
    {:ok, operation_supervisor} = DynamicSupervisor.start_link(strategy: :one_for_one)
    token = make_ref()

    candidates = [endpoint(failing_server), endpoint(healthy_server)]

    {:ok, refresher} =
      DynamicSupervisor.start_child(
        operation_supervisor,
        {TopologyRefresher,
         owner: self(),
         token: token,
         candidates: candidates,
         connections: %{},
         connection_supervisor: connection_supervisor,
         username: nil,
         password: nil,
         client_name: "topology-refresher-test",
         endpoint_validator: nil,
         connection_strategy: :new,
         timeout: 1_000}
      )

    assert_receive {:ferricstore_topology_refreshed, ^refresher, ^token,
                    {:ok, %Topology{}, connection, _key, _capacity, nil}},
                   1_000

    assert Process.alive?(connection)
  end

  test "an abnormal candidate connection exit does not kill fallback refresh" do
    owner = self()

    {:ok, failing_server} =
      NativeServer.start_link(
        owner: owner,
        response_fun: fn
          %{opcode: 0x0007} ->
            send(owner, :failing_candidate_blocked)
            :noreply

          %{opcode: opcode} when opcode in [0x0001, 0x000C] ->
            NativeServer.startup_payload()

          _request ->
            "OK"
        end
      )

    {:ok, healthy_server} = NativeServer.start_link(owner: owner)
    {:ok, connection_supervisor} = DynamicSupervisor.start_link(strategy: :one_for_one)
    {:ok, operation_supervisor} = DynamicSupervisor.start_link(strategy: :one_for_one)
    token = make_ref()

    {:ok, refresher} =
      DynamicSupervisor.start_child(
        operation_supervisor,
        {TopologyRefresher,
         owner: owner,
         token: token,
         candidates: [endpoint(failing_server), endpoint(healthy_server)],
         connections: %{},
         connection_supervisor: connection_supervisor,
         username: nil,
         password: nil,
         client_name: "topology-refresher-test",
         endpoint_validator: nil,
         connection_strategy: :new,
         timeout: 5_000}
      )

    assert_receive :failing_candidate_blocked, 2_000

    [{_id, failing_connection, :worker, _modules}] =
      DynamicSupervisor.which_children(connection_supervisor)

    Process.exit(failing_connection, :kill)

    assert_receive {:ferricstore_topology_refreshed, ^refresher, ^token,
                    {:ok, %Topology{}, healthy_connection, _key, _capacity, nil}},
                   3_000

    assert Process.alive?(healthy_connection)
  end

  test "a bootstrap call exit cannot leak its newly started connection" do
    owner = self()

    {:ok, server} =
      NativeServer.start_link(
        owner: owner,
        response_fun: fn
          %{opcode: 0x0007} ->
            send(owner, {:release_topology_response, self()})

            receive do
              :release -> NativeServer.topology_payload(1)
            end

          %{opcode: opcode} when opcode in [0x0001, 0x000C] ->
            NativeServer.startup_payload()

          _request ->
            "OK"
        end
      )

    {:ok, connection_supervisor} = DynamicSupervisor.start_link(strategy: :one_for_one)
    {:ok, operation_supervisor} = DynamicSupervisor.start_link(strategy: :one_for_one)
    token = make_ref()

    on_exit(fn ->
      for {_id, connection, _type, _modules} <- supervised_children(connection_supervisor),
          is_pid(connection) do
        if Process.alive?(connection), do: :sys.resume(connection)
      end
    end)

    {:ok, refresher} =
      DynamicSupervisor.start_child(
        operation_supervisor,
        {TopologyRefresher,
         owner: self(),
         token: token,
         candidates: [endpoint(server)],
         connections: %{},
         connection_supervisor: connection_supervisor,
         username: nil,
         password: nil,
         client_name: "topology-refresher-test",
         endpoint_validator: nil,
         connection_strategy: :new,
         timeout: 1_000}
      )

    assert_receive {:release_topology_response, handler}, 2_000

    [{_id, connection, :worker, _modules}] =
      DynamicSupervisor.which_children(connection_supervisor)

    :ok = :sys.suspend(connection)
    send(handler, :release)

    assert_receive {:ferricstore_topology_refreshed, ^refresher, ^token, {:error, _reason}},
                   2_500

    assert DynamicSupervisor.which_children(connection_supervisor) == []
  end

  defp endpoint(server) do
    %{
      host: "127.0.0.1",
      native_port: NativeServer.port(server),
      tls: false
    }
  end

  defp supervised_children(supervisor) do
    DynamicSupervisor.which_children(supervisor)
  catch
    :exit, _reason -> []
  end
end
