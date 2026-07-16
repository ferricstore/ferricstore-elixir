defmodule FerricStore.TLSTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.Topology
  alias FerricStore.Transport.TLS

  test "verified connections include hostname validation, SNI, and configured roots" do
    roots = [<<1, 2, 3>>]

    options = TLS.options(%{host: "db.example.test", verify: true, cacerts: roots})

    assert options[:verify] == :verify_peer
    assert options[:server_name_indication] == ~c"db.example.test"
    assert options[:cacerts] == roots
    assert [match_fun: match_fun] = options[:customize_hostname_check]
    assert is_function(match_fun, 2)
  end

  test "unwraps prepared CA certificates for the SSL transport" do
    roots = [:crypto.strong_rand_bytes(32)]

    config =
      Topology.prepare_endpoint(%{
        host: "db.example.test",
        verify: true,
        cacerts: roots
      })

    assert TLS.options(config)[:cacerts] == roots
  end

  test "an explicit server name and CA file are preserved for keyword clients" do
    options =
      TLS.options(
        host: "10.0.0.8",
        server_name: "db.internal.test",
        cacertfile: "/tmp/test-ca.pem"
      )

    assert options[:server_name_indication] == ~c"db.internal.test"
    assert options[:cacertfile] == "/tmp/test-ca.pem"
  end

  test "verification can only be disabled explicitly" do
    options = TLS.options(%{host: "localhost", verify: false})

    assert options[:verify] == :verify_none
    refute Keyword.has_key?(options, :customize_hostname_check)
    refute Keyword.has_key?(options, :cacerts)
  end
end
