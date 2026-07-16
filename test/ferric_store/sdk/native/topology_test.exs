defmodule FerricStore.SDK.Native.TopologyTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.Topology
  alias FerricStore.Transport.CACerts

  test "the topology type returned by the public SDK is documented" do
    assert {:docs_v1, _annotation, _language, _format, module_doc, _metadata, _docs} =
             Code.fetch_docs(Topology)

    refute module_doc in [:hidden, :none]
  end

  test "uses stable crc32 slots compatible with the server" do
    assert Topology.slot_for_key("123456789") == 294
    assert Topology.slot_for_key("plain") == 719
    assert Topology.slot_for_key("{user:42}:session") == 390
    assert Topology.slot_for_key("{}empty") == 873
  end

  test "hash tags colocate related keys" do
    assert Topology.slot_for_key("{tenant:1}:a") == Topology.slot_for_key("{tenant:1}:b")
    refute Topology.slot_for_key("{tenant:1}:a") == Topology.slot_for_key("{tenant:2}:a")
  end

  test "connection keys use the effective transport and TLS port" do
    tls_seed = %{host: "cache.internal", native_port: 6389, tls: true}

    tls_route = %{
      host: "cache.internal",
      native_port: 6388,
      native_tls_port: 6389,
      tls: true
    }

    plaintext = %{host: "cache.internal", native_port: 6389, tls: false}

    assert Topology.endpoint_key(tls_seed) == Topology.endpoint_key(tls_route)

    assert Topology.endpoint_key(Map.put(tls_seed, :server_name, nil)) ==
             Topology.endpoint_key(tls_seed)

    refute Topology.endpoint_key(tls_seed) == Topology.endpoint_key(plaintext)
  end

  test "connection keys include operational policy" do
    endpoint = %{host: "cache.internal", native_port: 6_389, tls: false}

    refute Topology.endpoint_key(endpoint) ==
             Topology.endpoint_key(Map.put(endpoint, :heartbeat_interval, :infinity))

    refute Topology.endpoint_key(endpoint) ==
             Topology.endpoint_key(Map.put(endpoint, :max_response_bytes, 1_024))
  end

  test "prepared custom CA identity is reused on the endpoint-key hot path" do
    certificate = :crypto.strong_rand_bytes(256)
    cacerts = List.duplicate(certificate, 512)
    endpoint = %{host: "cache.internal", native_port: 6_389, tls: true, cacerts: cacerts}
    prepared = Topology.prepare_endpoint(endpoint)

    assert %CACerts{certificates: ^cacerts, fingerprint: fingerprint} = prepared.cacerts
    assert fingerprint == :crypto.hash(:sha256, :erlang.term_to_binary(cacerts))
    assert Topology.prepare_endpoint(prepared) == prepared
    assert Topology.endpoint_key(prepared) == Topology.endpoint_key(endpoint)
  end

  test "builds slot routing table from SHARDS payload" do
    payload = %{
      "route_epoch" => 123,
      "shard_count" => 2,
      "ranges" => [
        %{
          "first_slot" => 0,
          "last_slot" => 511,
          "shard" => 0,
          "lane_id" => 1,
          "leader_node" => "a@host",
          "endpoint" => %{"node" => "a@host", "host" => "a.local", "native_port" => 6388}
        },
        %{
          "first_slot" => 512,
          "last_slot" => 1023,
          "shard" => 1,
          "lane_id" => 2,
          "leader_node" => "b@host",
          "endpoint" => %{"node" => "b@host", "host" => "b.local", "native_port" => 6388}
        }
      ]
    }

    assert {:ok, topology} = Topology.build(payload)
    assert {:ok, route0} = Topology.route_key(topology, "123456789")
    assert route0.shard == 0
    assert route0.endpoint.host == "a.local"

    assert {:ok, route1} = Topology.route_key(topology, "plain")
    assert route1.shard == 1
    assert route1.endpoint.host == "b.local"
  end

  test "uses seed endpoint host when SHARDS range omits advertised host" do
    payload = %{
      "route_epoch" => 123,
      "shard_count" => 1,
      "ranges" => [
        %{
          "first_slot" => 0,
          "last_slot" => 1023,
          "shard" => 0,
          "lane_id" => 1,
          "owner_node" => "ferricstore@container",
          "native_port" => 6388
        }
      ]
    }

    assert {:ok, topology} =
             Topology.build(payload,
               default_endpoint: %{host: "127.0.0.1", native_port: 6388}
             )

    assert {:ok, route} = Topology.route_key(topology, "plain")
    assert route.endpoint.host == "127.0.0.1"
    assert route.endpoint.native_port == 6388
    assert route.leader_node == "ferricstore@container"
  end

  test "inherits the configured TLS port when a SHARDS range omits it" do
    default_endpoint = %{
      host: "127.0.0.1",
      native_port: 6_388,
      native_tls_port: 6_389,
      tls: true
    }

    assert {:ok, topology} =
             Topology.build(single_shard_payload(), default_endpoint: default_endpoint)

    assert {:ok, route} = Topology.route_key(topology, "plain")
    assert route.endpoint.native_tls_port == 6_389
    assert route.endpoint.tls == true
    assert Topology.endpoint_key(route.endpoint) == Topology.endpoint_key(default_endpoint)
  end

  test "leader_unknown ranges return a specific topology error" do
    payload = %{
      "route_epoch" => 124,
      "shard_count" => 1,
      "ranges" => [
        %{
          "first_slot" => 0,
          "last_slot" => 1023,
          "shard" => 0,
          "lane_id" => 1,
          "owner_node" => nil,
          "leader_node" => nil,
          "hint" => "leader_unknown"
        }
      ]
    }

    assert {:error, {:leader_unknown, 0}} = Topology.build(payload)
  end

  test "rejects invalid or incomplete topology snapshots atomically" do
    valid = single_shard_payload()

    invalid_payloads = [
      put_in(valid, ["ranges", Access.at(0), "endpoint", "native_port"], 70_000),
      put_in(valid, ["ranges", Access.at(0), "lane_id"], -1),
      put_in(valid, ["ranges", Access.at(0), "lane_id"], 0),
      put_in(valid, ["ranges", Access.at(0), "shard"], -1),
      Map.put(valid, "shard_count", 99),
      Map.put(valid, "ranges", [:not_a_range_map]),
      put_in(valid, ["ranges", Access.at(0), "last_slot"], 1_022),
      Map.update!(valid, "ranges", fn [range] ->
        [Map.put(range, "last_slot", 700), %{range | "first_slot" => 700}]
      end)
    ]

    Enum.each(invalid_payloads, fn payload ->
      assert {:error, _reason} = Topology.build(payload)
    end)
  end

  test "rejects improper topology range lists without raising" do
    payload = single_shard_payload()
    [range] = payload["ranges"]
    payload = %{payload | "ranges" => [range | :invalid_tail]}

    assert {:error, :invalid_shards_payload} = Topology.build(payload)

    atom_payload = %{
      route_epoch: payload["route_epoch"],
      shard_count: payload["shard_count"],
      ranges: [range | :invalid_tail]
    }

    assert {:error, :improper_list} = Topology.build(atom_payload)
  end

  test "rejects invalid route key types without raising" do
    assert {:error, {:invalid_route_key, :not_binary}} =
             Topology.route_key(%Topology{}, :not_binary)
  end

  test "rejects route keys beyond the server key-size contract without echoing them" do
    oversized = :binary.copy("k", 65_536)

    assert {:error, {:invalid_route_key, %{reason: :too_large, bytes: 65_536, limit: 65_535}}} =
             Topology.route_key(%Topology{}, oversized)

    assert {:error, {:unmapped_slot, _slot}} =
             Topology.route_key(%Topology{}, :binary.copy("k", 65_535))
  end

  test "rejects atom and string topology keys that normalize to the same field" do
    base = single_shard_payload()

    payload = %{
      :ranges => base["ranges"],
      :shard_count => base["shard_count"],
      :route_epoch => 2,
      "route_epoch" => base["route_epoch"]
    }

    assert {:error, {:duplicate_normalized_map_key, "route_epoch"}} = Topology.build(payload)
  end

  test "builds the maximally fragmented slot table within a bounded reduction budget" do
    ranges =
      Enum.map(0..1_023, fn slot ->
        %{
          "first_slot" => slot,
          "last_slot" => slot,
          "shard" => slot,
          "lane_id" => slot + 1,
          "host" => "127.0.0.1",
          "native_port" => 6_388
        }
      end)

    payload = %{"route_epoch" => 1, "shard_count" => 1_024, "ranges" => ranges}
    {:reductions, before_build} = Process.info(self(), :reductions)

    assert {:ok, %Topology{}} = Topology.build(payload)

    {:reductions, after_build} = Process.info(self(), :reductions)
    assert after_build - before_build < 100_000
  end

  test "rejects range collections larger than the slot space before preparing entries" do
    range = %{
      "first_slot" => 0,
      "last_slot" => 0,
      "shard" => 0,
      "lane_id" => 1,
      "host" => "127.0.0.1",
      "native_port" => 6_388
    }

    payload = %{
      "route_epoch" => 1,
      "shard_count" => 1,
      "ranges" => List.duplicate(range, 100_000)
    }

    {:reductions, before_build} = Process.info(self(), :reductions)

    assert {:error, {:topology_too_large, %{limit: 1_024, field: "ranges"}}} =
             Topology.build(payload)

    {:reductions, after_build} = Process.info(self(), :reductions)
    assert after_build - before_build < 25_000
  end

  defp single_shard_payload do
    %{
      "route_epoch" => 1,
      "shard_count" => 1,
      "ranges" => [
        %{
          "first_slot" => 0,
          "last_slot" => 1_023,
          "shard" => 0,
          "lane_id" => 1,
          "endpoint" => %{
            "node" => "node@host",
            "host" => "127.0.0.1",
            "native_port" => 6_388
          }
        }
      ]
    }
  end
end
