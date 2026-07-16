defmodule FerricStore.SDK.KVValueContractTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.KV
  alias FerricStore.SDK.Native.{AdmissionGate, KVBatchPreparer}
  alias FerricStore.Test.ClientRuntime

  defmodule CaptureClient do
    use GenServer

    def start_link(owner),
      do: GenServer.start_link(__MODULE__, owner) |> ClientRuntime.wrap()

    @impl true
    def init(owner), do: {:ok, owner}

    @impl true
    def handle_call({:admitted_submission, %AdmissionGate{} = gate, request}, from, owner) do
      :ok = AdmissionGate.release(gate)
      handle_call(request, from, owner)
    end

    def handle_call(request, _from, owner) do
      send(owner, {:dispatched, request})
      {:reply, {:ok, "OK"}, owner}
    end
  end

  setup do
    {:ok, client} = CaptureClient.start_link(self())
    %{client: client}
  end

  test "string-value commands reject non-binary values before dispatch", %{client: client} do
    calls = [
      {:set, :value, fn -> KV.set(client, "key", 1) end},
      {:cas, :expected, fn -> KV.cas(client, "key", 1, "new") end},
      {:cas, :value, fn -> KV.cas(client, "key", "old", 1) end},
      {:fetch_or_compute_result, :value,
       fn -> KV.fetch_or_compute_result(client, "key", "token", 1, 1_000) end}
    ]

    for {operation, field, call} <- calls do
      assert {:error,
              {:invalid_kv_input,
               %{operation: ^operation, field: ^field, reason: :expected_binary}}} = call.()
    end

    refute_received {:dispatched, _request}
  end

  test "mset rejects non-binary values in every supported pair representation", %{client: client} do
    inputs = [
      [{"tuple", 1}],
      [["list", 1]],
      [%{"key" => "string-map", "value" => 1}],
      [%{key: "atom-map", value: 1}]
    ]

    for input <- inputs do
      assert {:error, {:invalid_mset_pair, %{reason: :expected_binary_value}}} =
               KV.mset(client, input)
    end

    refute_received {:dispatched, _request}
  end

  test "mset map values are rejected by the O(1) admission callback before wire encoding" do
    {key_fun, _payload_builder} = KVBatchPreparer.callbacks(:mset)

    assert {:error, {:invalid_mset_pair, %{reason: :expected_binary_value}}} =
             key_fun.({"container-map", 1})
  end

  test "binary contract errors do not retain a rejected value", %{client: client} do
    marker = String.duplicate("sensitive-value", 10_000)
    rejected = [marker]

    assert {:error, {:invalid_kv_input, details}} = KV.set(client, "key", rejected)
    refute Map.has_key?(details, :value)
    refute inspect(details) =~ marker
    refute_received {:dispatched, _request}
  end

  test "hset rejects non-binary fields and values before dispatch", %{client: client} do
    cases = [
      {%{:field => "value"}, :expected_binary_field},
      {%{"field" => 1}, :expected_binary_value},
      {%{"field" => self()}, :expected_binary_value}
    ]

    for {fields, reason} <- cases do
      assert {:error, {:invalid_kv_input, %{operation: :hset, field: :fields, reason: ^reason}}} =
               KV.hset(client, "hash", fields)
    end

    refute_received {:dispatched, _request}
  end
end
