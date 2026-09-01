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

  test "release publication is guarded by tag-version identity and Hex immutability" do
    workflow = File.read!(".github/workflows/release.yml")
    guard = File.read!("scripts/verify-release-tag.sh")

    assert workflow =~ ~s(tags:\n      - "v*.*.*")
    assert workflow =~ "concurrency:"
    assert workflow =~ "cancel-in-progress: false"
    assert workflow =~ "id: release_guard"
    assert workflow =~ "scripts/verify-release-tag.sh"
    assert workflow =~ "steps.release_guard.outputs.already_published != 'true'"

    guard_position = byte_offset(workflow, "scripts/verify-release-tag.sh")
    publish_position = byte_offset(workflow, "mix hex.publish --yes")
    assert guard_position < publish_position

    assert guard =~ "GITHUB_REF_TYPE"
    assert guard =~ "GITHUB_REF_NAME"
    assert guard =~ "mix.exs version"
    assert guard =~ "/packages/${package}/releases/${expected_version}"
    assert guard =~ ~s(jq -r '.checksum')
    assert guard =~ "mix hex.build --output"
    assert guard =~ "local_checksum"
    assert guard =~ "published_checksum"
    assert guard =~ "different package checksum"
    assert guard =~ "already_published=true"
    assert guard =~ "already_published=false"
  end

  test "release guard rejects mismatched tags and makes published retries a no-op" do
    base_env = [
      {"PROJECT_VERSION", "0.12.0"},
      {"GITHUB_REF_TYPE", "tag"},
      {"CHECK_HEX", "false"}
    ]

    assert {output, 0} =
             System.cmd("bash", ["scripts/verify-release-tag.sh"],
               env: [{"RELEASE_TAG", "v0.12.0"} | base_env],
               stderr_to_stdout: true
             )

    assert output =~ "already_published=false"

    assert {output, 1} =
             System.cmd("bash", ["scripts/verify-release-tag.sh"],
               env: [{"RELEASE_TAG", "v0.12.1"} | base_env],
               stderr_to_stdout: true
             )

    assert output =~ "does not match mix.exs version"

    fake_bin =
      Path.join(
        System.tmp_dir!(),
        "ferricstore-release-guard-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(fake_bin)
    fake_curl = Path.join(fake_bin, "curl")

    File.write!(
      fake_curl,
      """
      #!/usr/bin/env sh
      while [ "$#" -gt 0 ]; do
        if [ "$1" = "--output" ]; then
          shift
          printf '{"checksum":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}' > "$1"
        fi
        shift
      done
      printf 200
      """
    )

    File.chmod!(fake_curl, 0o700)
    on_exit(fn -> File.rm_rf!(fake_bin) end)

    assert {output, 0} =
             System.cmd("bash", ["scripts/verify-release-tag.sh"],
               env: [
                 {"RELEASE_TAG", "v0.12.0"},
                 {"PROJECT_VERSION", "0.12.0"},
                 {"GITHUB_REF_TYPE", "tag"},
                 {"CHECK_HEX", "true"},
                 {"LOCAL_PACKAGE_CHECKSUM",
                  "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
                 {"PATH", fake_bin <> ":" <> System.fetch_env!("PATH")}
               ],
               stderr_to_stdout: true
             )

    assert output =~ "already_published=true"
    assert output =~ "skipping immutable publish"

    assert {output, 1} =
             System.cmd("bash", ["scripts/verify-release-tag.sh"],
               env: [
                 {"RELEASE_TAG", "v0.12.0"},
                 {"PROJECT_VERSION", "0.12.0"},
                 {"GITHUB_REF_TYPE", "tag"},
                 {"CHECK_HEX", "true"},
                 {"LOCAL_PACKAGE_CHECKSUM",
                  "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
                 {"PATH", fake_bin <> ":" <> System.fetch_env!("PATH")}
               ],
               stderr_to_stdout: true
             )

    assert output =~ "different package checksum"
  end

  defp byte_offset(contents, needle) do
    {position, _length} = :binary.match(contents, needle)
    position
  end
end
