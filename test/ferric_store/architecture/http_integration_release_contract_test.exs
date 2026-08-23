defmodule FerricStore.Architecture.HTTPIntegrationReleaseContractTest do
  use ExUnit.Case, async: true

  test "CI and release require authenticated TLS HTTP integration" do
    runner = File.read!("scripts/test_http_integration.sh")

    runner
    |> String.split("\n")
    |> Enum.filter(&String.contains?(&1, "quay.io/ferricstore/ferricstore:"))
    |> Enum.each(&assert String.contains?(&1, "@sha256:"))

    for required <- [
          "FERRICSTORE_HTTP_TLS_ENABLED=true",
          "FERRICSTORE_USERNAME",
          "FERRICSTORE_PASSWORD",
          "FERRICSTORE_CA_FILE",
          "@sha256:",
          "chmod 700",
          "chmod 600",
          ~s(rm -f "$tls_dir/ca.key"),
          "source=$tls_dir/server.key,target=/tls/server.key,readonly",
          "sdk-http-denied",
          "ACL authorization probe unexpectedly allowed SET",
          "unauthenticated HTTP request returned",
          "client_integration_test.exs"
        ] do
      assert runner =~ required
    end

    for workflow <- [".github/workflows/ci.yml", ".github/workflows/release.yml"] do
      contents = File.read!(workflow)
      assert contents =~ "scripts/test_http_integration.sh"
      assert contents =~ "@sha256:"
      assert contents =~ "mix hex.audit"

      contents
      |> String.split("\n")
      |> Enum.filter(&String.contains?(&1, "quay.io/ferricstore/ferricstore:"))
      |> Enum.each(&assert String.contains?(&1, "@sha256:"))
    end

    assert File.read!("mix.exs") =~ ~s({:bandit, "~> 1.12.4", only: :test})
    refute File.read!("mix.lock") =~ ~s("cowlib":)

    readme = File.read!("README.md")
    assert readme =~ "test_http_integration.sh"
    assert readme =~ "FERRICSTORE_CA_FILE"
  end
end
