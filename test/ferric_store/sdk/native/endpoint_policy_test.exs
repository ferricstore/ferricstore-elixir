defmodule FerricStore.SDK.Native.EndpointPolicyTest do
  use ExUnit.Case, async: false

  alias FerricStore.SDK.Native.{EndpointPolicy, EndpointTrust}

  test "seed normalization rejects ambiguous endpoint keys" do
    assert {:error, {:invalid_endpoint, endpoint}} =
             EndpointPolicy.normalize(%{"host" => "second", host: "first", native_port: 6_381})

    assert endpoint.host == "first"
    assert endpoint["host"] == "second"
  end

  test "endpoint normalization canonicalizes valid hosts and rejects unsafe names" do
    assert {:ok, %{host: "seed.example"}} =
             EndpointPolicy.normalize({" Seed.Example ", 6_388})

    for host <- [<<0xFF>>, String.duplicate("a", 256), "   "] do
      endpoint = %{host: host, native_port: 6_388}
      assert {:error, {:invalid_endpoint, ^endpoint}} = EndpointPolicy.normalize(endpoint)
    end
  end

  test "endpoint normalization accepts and canonicalizes generic ports" do
    assert {:ok, %{host: "seed.example", port: 6_388, native_port: 6_388}} =
             EndpointPolicy.normalize(%{host: " Seed.Example ", port: 6_388})
  end

  test "endpoint normalization accepts TLS-only native ports" do
    assert {:ok,
            %{
              host: "seed.example",
              native_port: 6_389,
              native_tls_port: 6_389,
              tls: true
            }} =
             EndpointPolicy.normalize(%{
               host: " Seed.Example ",
               native_tls_port: 6_389,
               tls: true
             })
  end

  test "endpoint normalization rejects unknown options instead of retaining them" do
    endpoint = %{
      host: "127.0.0.1",
      native_port: 6_388,
      max_inflight: 1,
      password: "must-not-enter-transport-state"
    }

    assert {:error, {:invalid_endpoint, ^endpoint}} = EndpointPolicy.normalize(endpoint)
  end

  test "over-wide endpoint maps are rejected before key-list allocation" do
    endpoint =
      1..100_000
      |> Map.new(fn index -> {index, index} end)
      |> Map.merge(%{host: "127.0.0.1", native_port: 6_388})

    assert {:ok, _endpoint} = EndpointPolicy.normalize({"127.0.0.1", 6_388})
    :erlang.garbage_collect(self())
    {:reductions, before_reductions} = Process.info(self(), :reductions)

    result = EndpointPolicy.normalize(endpoint)

    {:reductions, after_reductions} = Process.info(self(), :reductions)
    assert {:error, {:invalid_endpoint, ^endpoint}} = result
    assert after_reductions - before_reductions < 5_000
  end

  test "seed-host policy compares normalized host names" do
    trusted_hosts =
      EndpointTrust.new([%{host: "Seed.EXAMPLE ", native_port: 6_388}], [])

    assert :ok =
             EndpointPolicy.validate(
               :seed_hosts,
               trusted_hosts,
               nil,
               %{host: " seed.example"},
               10
             )

    assert {:error, :unsafe_endpoint} =
             EndpointPolicy.validate(:seed_hosts, trusted_hosts, nil, %{host: "other"}, 10)
  end

  test "none policy permits only exact configured seed endpoints" do
    seed = %{host: "Seed.EXAMPLE", native_port: 6_388, tls: false}
    trusted = EndpointTrust.new([seed], trusted_hosts: ["other.example"])

    assert :ok = EndpointPolicy.validate_policy(:none, trusted, seed)

    assert {:error, :unsafe_endpoint} =
             EndpointPolicy.validate_policy(
               :none,
               trusted,
               %{seed | native_port: 6_389}
             )

    assert {:error, :unsafe_endpoint} =
             EndpointPolicy.validate_policy(:none, trusted, %{seed | tls: true})

    assert {:error, :unsafe_endpoint} =
             EndpointPolicy.validate_policy(
               :none,
               trusted,
               %{host: "other.example", native_port: 6_388, tls: false}
             )
  end

  test "allow-host policies are compiled once for constant-time runtime checks" do
    hosts = Enum.map(1..1_024, &"host-#{&1}.example")
    policy = EndpointPolicy.compile({:allow_hosts, hosts})
    trusted = EndpointTrust.new([%{host: "seed.example", native_port: 6_388}], [])
    endpoint = %{host: "host-1024.example"}

    :erlang.garbage_collect(self())
    {:reductions, before_reductions} = Process.info(self(), :reductions)

    Enum.each(1..1_000, fn _iteration ->
      assert :ok = EndpointPolicy.validate_policy(policy, trusted, endpoint)
    end)

    {:reductions, after_reductions} = Process.info(self(), :reductions)
    assert after_reductions - before_reductions < 250_000
  end

  test "configured endpoint options fill missing values without overriding topology data" do
    defaults = EndpointPolicy.options(server_name: "default", connect_timeout: 200)

    assert %{server_name: "topology", connect_timeout: 200} =
             EndpointPolicy.apply_options(%{server_name: "topology"}, defaults)
  end
end
