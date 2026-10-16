defmodule FerricStore.Architecture.DocumentationContractTest do
  use ExUnit.Case, async: true

  @root Path.expand("../../..", __DIR__)

  test "installation examples track the package version" do
    version = Mix.Project.config() |> Keyword.fetch!(:version)

    for path <- ["README.md", "docs/quickstart.md"] do
      contents = File.read!(Path.join(@root, path))
      assert contents =~ ~s({:ferricstore_sdk, "~> #{version}"})
    end
  end

  test "the release requires the projection-capable FerricStore beta without changing wire v1" do
    assert Mix.Project.config()[:version] == "0.12.5"
    assert FerricStore.minimum_server_version() == "0.11.4"
    assert FerricStore.SDK.minimum_server_version() == "0.11.4"
    assert FerricStore.server_version_requirement() == "~> 0.11.4 or ~> 0.12.0"
    assert FerricStore.SDK.server_version_requirement() == "~> 0.11.4 or ~> 0.12.0"
    assert FerricStore.Compatibility.protocol_version() == 1

    # The SDK is validated against the 0.11.4 floor and the 0.12.0 release; an
    # unvalidated later minor stays outside the requirement.
    requirement = Version.parse_requirement!(FerricStore.server_version_requirement())

    for version <- ["0.11.4", "0.11.24", "0.12.0", "0.12.3"],
        do: assert(Version.match?(version, requirement), version)

    for version <- ["0.11.3", "0.13.0"],
        do: refute(Version.match?(version, requirement), version)

    compatibility = File.read!(Path.join(@root, "lib/ferric_store/compatibility.ex"))
    assert compatibility =~ "SDK 0.12.5 requires server"

    for path <- ["README.md", "docs/quickstart.md"] do
      assert path |> then(&File.read!(Path.join(@root, &1))) =~
               "FerricStore `~> 0.11.4 or ~> 0.12.0`"
    end
  end

  test "development guidance points at the current architecture suite" do
    contents = File.read!(Path.join(@root, "docs/development.md"))

    assert contents =~ "test/ferric_store/architecture/"
    refute contents =~ "test/ferric_store/architecture_test.exs"
  end

  test "workflow timing documentation separates server and client clocks" do
    contents = File.read!(Path.join(@root, "docs/workflow.md"))

    assert contents =~ "server time"
    assert contents =~ "client wall clock"
    assert contents =~ "Omit `now_ms` in production"
  end

  test "workflow guides cover durable replay, external idempotency, waiting handoff, and migration" do
    workflow = File.read!(Path.join(@root, "docs/workflow.md"))
    quickstart = File.read!(Path.join(@root, "docs/quickstart.md"))

    for required <- [
          "FerricStore.Workflow.advance(workflow, job",
          "FerricStore.Workflow.step(workflow, job",
          "The step name is a stable replay identity",
          "External providers still need a stable idempotency key",
          "A waiting workflow does not occupy a worker",
          "any available worker can acquire a fresh lease",
          "`step_continue/3` remains available only as a deprecated low-level migration API"
        ] do
      assert workflow =~ required
    end

    assert quickstart =~ "FerricStore.Workflow.step(workflow, job"
    assert quickstart =~ "idempotency_key"
  end

  test "the Hex package carries the complete declared Apache license" do
    contents = File.read!(Path.join(@root, "LICENSE"))

    assert byte_size(contents) > 10_000
    assert contents =~ "TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION"
    assert contents =~ "END OF TERMS AND CONDITIONS"
  end

  test "public documentation does not link through hidden implementation modules" do
    flow = File.read!(Path.join(@root, "lib/ferric_store/flow.ex"))
    protocol = File.read!(Path.join(@root, "lib/ferric_store/protocol.ex"))
    topology = File.read!(Path.join(@root, "lib/ferric_store/sdk/native/topology.ex"))

    refute flow =~ "defdelegate"
    refute protocol =~ "FrameCodec.frame()"
    refute topology =~ "defdelegate slot_for_key"
  end
end
