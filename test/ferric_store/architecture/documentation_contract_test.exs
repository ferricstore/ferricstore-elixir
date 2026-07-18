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

  test "the release declares the FerricStore 0.8 beta contract without changing wire v1" do
    assert Mix.Project.config()[:version] == "0.3.0"
    assert FerricStore.minimum_server_version() == "0.8.0"
    assert FerricStore.SDK.minimum_server_version() == "0.8.0"
    assert FerricStore.Compatibility.protocol_version() == 1

    for path <- ["README.md", "docs/quickstart.md"] do
      assert path |> then(&File.read!(Path.join(@root, &1))) =~ "FerricStore 0.8.0 or newer"
    end
  end

  test "development guidance points at the current architecture suite" do
    contents = File.read!(Path.join(@root, "docs/development.md"))

    assert contents =~ "test/ferric_store/architecture/"
    refute contents =~ "test/ferric_store/architecture_test.exs"
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
