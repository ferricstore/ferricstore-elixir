defmodule FerricStore.Flow.QueryOptionContractTest do
  use ExUnit.Case, async: true

  alias FerricStore.Flow
  alias FerricStore.Test.ClientRuntime

  defmodule CaptureClient do
    use GenServer

    def start_link(owner) do
      GenServer.start_link(__MODULE__, owner)
      |> ClientRuntime.wrap()
    end

    @impl true
    def init(owner), do: {:ok, owner}

    @impl true
    def handle_call({:admitted_submission, gate, request}, from, owner) do
      :ok = ClientRuntime.release_submission(gate)
      handle_call(request, from, owner)
    end

    def handle_call({:request, _opcode, payload, _context}, _from, owner) do
      send(owner, {:flow_payload, payload})
      {:reply, {:ok, query_response()}, owner}
    end

    defp query_response do
      %{
        "version" => "ferric.flow.query.result/v1",
        "records" => [],
        "page" => %{"has_more" => false, "cursor" => nil},
        "quality" => %{
          "exactness" => "exact",
          "freshness" => "authoritative",
          "coverage" => "complete",
          "pagination" => "stable"
        },
        "usage" => %{
          "range_seeks" => 0,
          "range_pages" => 0,
          "scanned_entries" => 0,
          "scanned_bytes" => 0,
          "hydrated_records" => 0,
          "residual_checks" => 0,
          "duplicate_entries" => 0,
          "result_records" => 0,
          "response_bytes" => 0,
          "memory_high_water_bytes" => 0,
          "wall_time_us" => 0
        }
      }
    end
  end

  test "list rejects unsupported return modes before transport" do
    {:ok, client} = CaptureClient.start_link(self())

    assert {:error,
            %FerricStore.Error{
              raw: {:invalid_flow_option, :list, :return, :expected_meta_return}
            }} =
             Flow.list(client,
               type: "email",
               partition_key: "tenant-a",
               return: :records
             )

    refute_received {:flow_payload, _payload}
  end

  test "list accepts the canonical meta return mode case-insensitively" do
    for return <- [:meta, "meta", "MeTa"] do
      {:ok, client} = CaptureClient.start_link(self())

      assert [] =
               Flow.list(client,
                 type: "email",
                 partition_key: "tenant-a",
                 return: return
               )

      assert_received {:flow_payload, %{"query" => query}}
      assert query =~ "FROM runs WHERE partition_key = @partition_key"
    end
  end
end
