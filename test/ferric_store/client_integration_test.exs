defmodule FerricStore.ClientIntegrationTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  setup do
    url = System.get_env("FERRICSTORE_URL", "ferric://127.0.0.1:6388")
    client = FerricStore.connect!(url: url, client_name: "ferricstore-elixir-test")

    on_exit(fn -> FerricStore.close(client) end)

    {:ok, client: client}
  end

  test "native KV commands", %{client: client} do
    key = "elixir-sdk-kv-#{System.unique_integer([:positive])}"

    assert :ok = FerricStore.set(client, key, "value")
    assert FerricStore.get(client, key) == "value"
    assert FerricStore.delete(client, key) in [1, "1"]
  end

  test "data structure helpers", %{client: client} do
    base = "elixir-sdk-ds-#{System.unique_integer([:positive])}"

    assert FerricStore.hset(client, base <> ":hash", "field", "value") in [1, "1", "OK"]
    assert FerricStore.hget(client, base <> ":hash", "field") == "value"

    assert is_integer(FerricStore.lpush(client, base <> ":list", ["a", "b"]))
    assert FerricStore.lrange(client, base <> ":list", 0, -1) == ["b", "a"]

    assert is_integer(FerricStore.sadd(client, base <> ":set", ["a", "b"]))
    assert FerricStore.sismember(client, base <> ":set", "a") in [1, true]

    assert FerricStore.zadd(client, base <> ":zset", 1, "a") in [1, "1", "OK"]
    assert FerricStore.zscore(client, base <> ":zset", "a") in [1, 1.0, "1", "1.0"]
  end

  test "flow create claim complete with value refs", %{client: client} do
    id = "elixir-sdk-flow-#{System.unique_integer([:positive])}"
    type = "elixir-sdk-#{System.unique_integer([:positive])}"
    worker = "worker-#{System.unique_integer([:positive])}"

    ref = FerricStore.Flow.value_put(client, "large-value")

    assert is_binary(ref) or is_map(ref)

    assert FerricStore.Flow.create(client, id,
             type: type,
             payload: "payload",
             attributes: %{tenant: "acme"},
             value_refs: %{blob: extract_ref(ref)},
             now_ms: System.system_time(:millisecond)
           ) in ["OK", "QUEUED", "CREATED"]

    jobs =
      FerricStore.Flow.claim_due(client, type,
        state: "queued",
        worker: worker,
        limit: 1
      )

    assert is_list(jobs)
    assert [job | _] = jobs

    assert FerricStore.Flow.complete(client, id,
             lease_token: Map.get(job, "lease_token"),
             fencing_token: Map.get(job, "fencing_token"),
             result: "done"
           ) in ["OK", "COMPLETED"]
  end

  defp extract_ref(%{"ref" => ref}), do: ref
  defp extract_ref(%{ref: ref}), do: ref
  defp extract_ref(ref) when is_binary(ref), do: ref
end
