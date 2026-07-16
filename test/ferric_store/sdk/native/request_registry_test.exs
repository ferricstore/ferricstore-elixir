defmodule FerricStore.SDK.Native.RequestRegistryTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.RequestRegistry

  test "indexes request tags by their active connection" do
    first_connection = spawn(fn -> Process.sleep(:infinity) end)
    second_connection = spawn(fn -> Process.sleep(:infinity) end)
    first_tag = make_ref()
    second_tag = make_ref()

    on_exit(fn ->
      Process.exit(first_connection, :kill)
      Process.exit(second_connection, :kill)
    end)

    registry =
      %RequestRegistry{}
      |> RequestRegistry.put(first_tag, %{conn: nil})
      |> RequestRegistry.put(second_tag, %{conn: second_connection})
      |> RequestRegistry.update!(first_tag, &Map.put(&1, :conn, first_connection))

    assert RequestRegistry.connection_tags(registry, first_connection) == MapSet.new([first_tag])

    assert RequestRegistry.connection_tags(registry, second_connection) ==
             MapSet.new([second_tag])

    {_request, registry} = RequestRegistry.pop(registry, first_tag)
    assert RequestRegistry.connection_tags(registry, first_connection) == MapSet.new()

    assert RequestRegistry.connection_tags(registry, second_connection) ==
             MapSet.new([second_tag])
  end

  test "indexes an async delivery reference without reusing it as the attempt tag" do
    owner = self()
    ref = make_ref()
    tag = RequestRegistry.request_tag(%{from: {:async, owner, ref}})
    request = %{conn: nil, from: {:async, owner, ref}}
    registry = RequestRegistry.put(%RequestRegistry{}, tag, request)

    refute tag == ref
    assert RequestRegistry.fetch_async(registry, owner, ref) == {:ok, tag, request}
    assert RequestRegistry.fetch_async(registry, spawn(fn -> :ok end), ref) == :error

    {^request, registry} = RequestRegistry.pop(registry, tag)
    assert RequestRegistry.fetch_async(registry, owner, ref) == :error
  end
end
