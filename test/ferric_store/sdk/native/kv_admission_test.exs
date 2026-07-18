defmodule FerricStore.SDK.Native.KVAdmissionTest do
  use ExUnit.Case, async: false

  alias FerricStore.ClientIdentity
  alias FerricStore.Protocol.PreparedMap
  alias FerricStore.SDK
  alias FerricStore.SDK.Native.{AdmissionGate, KVPreparedRequest, Topology}

  defmodule AdmissionClient do
    use GenServer

    def start_link(owner, admission, opts \\ []),
      do: GenServer.start_link(__MODULE__, {owner, admission, opts})

    @impl true
    def init({owner, admission, opts}) do
      endpoint = :ets.new(__MODULE__, [:set, :protected, read_concurrency: true])
      ClientIdentity.mark(:topology_aware, endpoint)
      {:ok, topology} = Topology.build(topology_payload())

      true =
        :ets.insert(endpoint, [
          {:client, self()},
          {:coordinator, self()},
          {:submission_admission, AdmissionGate.new(1_024)},
          {:topology, make_ref(), topology}
        ])

      {:ok, %{owner: owner, admission: admission, prepared_delay: opts[:prepared_delay] || 0}}
    end

    @impl true
    def handle_call({:admitted_submission, %AdmissionGate{} = gate, request}, from, state) do
      :ok = AdmissionGate.release(gate)
      handle_call(request, from, state)
    end

    def handle_call({:kv_preparation_admission, item_count, _context}, _from, state) do
      send(state.owner, {:kv_preparation_admission, item_count})

      case state.admission do
        :ok -> {:reply, {:ok, make_ref()}, state}
        {:error, _reason} = error -> {:reply, error, state}
      end
    end

    def handle_call(
          {:prepared_command_items,
           %KVPreparedRequest{
             reservation: reservation,
             operation: :mget,
             item_count: item_count,
             groups: groups
           }},
          _from,
          state
        )
        when is_reference(reservation) do
      send(state.owner, {:kv_prepared_submission_started, reservation})
      if state.prepared_delay > 0, do: Process.sleep(state.prepared_delay)
      send(state.owner, {:compact_prepared_items, item_count, groups})

      groups =
        Enum.map(groups, fn group ->
          %{items: keys} = PreparedMap.metadata(group.payload)
          Map.put(group, :value, keys)
        end)

      {:reply, {:ok, groups}, state}
    end

    @impl true
    def handle_cast({:release_kv_preparation, reservation, owner}, state) do
      send(state.owner, {:released_kv_preparation, reservation, owner})
      {:noreply, state}
    end

    defp topology_payload do
      %{
        "route_epoch" => 1,
        "shard_count" => 1,
        "ranges" => [
          %{
            "first_slot" => 0,
            "last_slot" => 1_023,
            "shard" => 0,
            "lane_id" => 1,
            "node" => "admission-test",
            "host" => "127.0.0.1",
            "native_port" => 6_388
          }
        ]
      }
    end
  end

  test "large trusted KV preparation is rejected before routing or payload allocation" do
    {:ok, client} = AdmissionClient.start_link(self(), {:error, :client_backpressure})
    keys = Enum.map(1..1_000, &"saturated-key-#{&1}")

    assert {:error, :client_backpressure} = SDK.mget(client, keys)
    assert_received {:kv_preparation_admission, 1_000}
    refute_received {:compact_prepared_items, _item_count, _groups}
  end

  test "large mset maps are rejected before pair-list materialization" do
    {:ok, client} = AdmissionClient.start_link(self(), {:error, :client_backpressure})
    warmup_pairs = Map.new(1..256, &{"warmup-key-#{&1}", &1})

    assert {:error, :client_backpressure} =
             SDK.mset(client, warmup_pairs)

    assert_received {:kv_preparation_admission, 256}

    pairs = Map.new(1..100_000, &{"saturated-key-#{&1}", &1})
    :erlang.garbage_collect(self())
    {:reductions, before_reductions} = Process.info(self(), :reductions)

    assert {:error, :client_backpressure} =
             SDK.mset(client, pairs)

    {:reductions, after_reductions} = Process.info(self(), :reductions)
    assert after_reductions - before_reductions < 50_000
    assert_received {:kv_preparation_admission, 100_000}
    refute_received {:compact_prepared_items, _item_count, _groups}
  end

  test "an expired deadline rejects large KV input before traversing it" do
    {:ok, client} = AdmissionClient.start_link(self(), :ok)
    assert {:error, :timeout} = SDK.mget(client, ["warmup"], timeout: 0, call_timeout: 0)

    keys = Enum.map(1..100_000, &"expired-key-#{&1}")
    :erlang.garbage_collect(self())
    {:reductions, before_reductions} = Process.info(self(), :reductions)

    result = SDK.mget(client, keys, timeout: 0, call_timeout: 0)

    {:reductions, after_reductions} = Process.info(self(), :reductions)
    assert {:error, :timeout} = result
    assert after_reductions - before_reductions < 20_000
    refute_received {:kv_preparation_admission, _item_count}
    refute_received {:compact_prepared_items, _item_count, _groups}
  end

  test "admitted trusted KV messages carry protocol-prepared payloads without duplicate group lists" do
    {:ok, client} = AdmissionClient.start_link(self(), :ok)
    keys = Enum.map(1..1_000, &"admitted-key-#{&1}")

    assert {:ok, ^keys} = SDK.mget(client, keys)
    assert_received {:kv_preparation_admission, 1_000}
    assert_received {:compact_prepared_items, 1_000, groups}
    assert Enum.sum(Enum.map(groups, &length(&1.indexes))) == 1_000
    refute Enum.any?(groups, &Map.has_key?(&1, :items))

    assert Enum.all?(groups, fn group ->
             match?(%PreparedMap{}, group.payload) and
               match?(
                 %{operation: :mget, items: items} when is_list(items),
                 PreparedMap.metadata(group.payload)
               )
           end)

    refute_received {:prepared_items_with_original, _items, _item_count, _groups}
  end

  test "failed prepared submission cleanup never extends the caller deadline" do
    {:ok, client} = AdmissionClient.start_link(self(), :ok, prepared_delay: 1_000)
    keys = Enum.map(1..256, &"deadline-key-#{&1}")
    started = System.monotonic_time(:millisecond)

    assert {:error, :timeout} = SDK.mget(client, keys, timeout: 500, call_timeout: 500)
    assert System.monotonic_time(:millisecond) - started < 650

    assert_receive {:kv_prepared_submission_started, reservation}, 500
    assert_receive {:released_kv_preparation, ^reservation, owner}, 1_500
    assert owner == self()
  end
end
