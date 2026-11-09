defmodule FerricStore.Flow.ChildrenTest do
  use ExUnit.Case, async: true

  alias FerricStore.Flow.{ChildGroupReader, ChildGroupResults, Children, ChildResult}

  @job %{
    "id" => "report-1",
    "partition_key" => "tenant-1",
    "lease_token" => "lease-1",
    "fencing_token" => 7,
    "state" => "running",
    "run_state" => "split"
  }

  @children [
    %{id: "report-1:part-1", type: "report-part", payload: %{part: 1}},
    [id: "report-1:part-2", type: "report-part", payload: %{part: 2}, partition_key: "tenant-2"]
  ]

  @states [wait_state: "waiting_for_parts", success: "join", failure: "failed"]

  test "the spawn payload carries the claim, the states, and encoded child payloads" do
    assert {:ok, payload} =
             Children.spawn_payload(
               @job,
               @children,
               @states ++
                 [
                   group_id: "parts",
                   wait: :any,
                   on_child_failed: :ignore,
                   on_parent_closed: "abandon_children",
                   codec: FerricStore.Codec.Term,
                   now_ms: 1_000
                 ]
             )

    assert payload == %{
             "id" => "report-1",
             "partition_key" => "tenant-1",
             "lease_token" => "lease-1",
             "fencing_token" => 7,
             "group_id" => "parts",
             "wait" => "any",
             "wait_state" => "waiting_for_parts",
             "success" => "join",
             "failure" => "failed",
             "on_child_failed" => "ignore",
             "on_parent_closed" => "abandon_children",
             "now_ms" => 1_000,
             "children" => [
               %{
                 "id" => "report-1:part-1",
                 "type" => "report-part",
                 "payload" => :erlang.term_to_binary(%{part: 1})
               },
               %{
                 "id" => "report-1:part-2",
                 "type" => "report-part",
                 "payload" => :erlang.term_to_binary(%{part: 2}),
                 "partition_key" => "tenant-2"
               }
             ]
           }
  end

  test "the spawn payload defaults to the default group, waiting for all children" do
    assert {:ok, payload} =
             Children.spawn_payload(@job, @children, [codec: FerricStore.Codec.Term] ++ @states)

    assert payload["group_id"] == "default"
    assert payload["wait"] == "all"
    refute Map.has_key?(payload, "on_child_failed")
  end

  test "incomplete waits are rejected before anything is sent" do
    for {opts, reason} <- [
          {Keyword.delete(@states, :wait_state), {:missing_child_wait_option, :wait_state}},
          {Keyword.delete(@states, :success), {:missing_child_wait_option, :success}},
          {Keyword.put(@states, :failure, ""), {:missing_child_wait_option, :failure}},
          {Keyword.put(@states, :wait, :none), {:invalid_child_wait, :none}}
        ] do
      assert Children.spawn_payload(@job, @children, opts) == {:error, reason}
    end

    assert Children.spawn_payload(@job, [], @states) == {:error, {:invalid_children, :empty}}

    assert Children.spawn_payload(@job, [%{id: "x"}], @states) ==
             {:error, {:invalid_child, %{id: "x"}}}

    assert Children.spawn_payload(%{"id" => "report-1"}, @children, @states) ==
             {:error, {:invalid_flow_claim, :wait_for_children, :expected_claimed_job}}
  end

  test "a group is read with children ordered by id and value refs in child order" do
    record = %{
      "id" => "report-1",
      "child_groups" => %{
        "parts" => %{
          "wait" => "all",
          "resolved" => "success",
          "children" => %{
            "report-1:part-2" => "failed",
            "report-1:part-1" => "completed",
            "report-1:part-3" => "running"
          },
          "child_partitions" => %{"report-1:part-1" => "tenant-1"},
          "results" => %{
            "report-1:part-1" => %{"result_ref" => "ref-result-1", "status" => "completed"},
            "report-1:part-2" => %{"error_ref" => "ref-error-2", "status" => "failed"}
          },
          "summary" => %{"total" => 3, "completed" => 1, "failed" => 1, "cancelled" => 0}
        }
      }
    }

    assert {:ok, group, refs} = ChildGroupReader.read(record, "parts")
    assert refs == ["ref-result-1", "ref-error-2"]

    assert ChildGroupReader.fill_values(group, ["rows=3", "timeout"]) == %ChildGroupResults{
             group_id: "parts",
             wait: "all",
             resolved: "success",
             summary: %{total: 3, completed: 1, failed: 1, cancelled: 0},
             children: [
               %ChildResult{
                 id: "report-1:part-1",
                 partition_key: "tenant-1",
                 status: "completed",
                 result: "rows=3"
               },
               %ChildResult{id: "report-1:part-2", status: "failed", error: "timeout"},
               %ChildResult{id: "report-1:part-3", status: "running"}
             ]
           }

    assert ChildGroupReader.read(record, "other") == {:ok, nil, []}
    assert ChildGroupReader.read(nil, "parts") == {:ok, nil, []}

    paused = put_in(record, ["child_groups", "parts", "children"], %{"a" => "paused"})
    assert ChildGroupReader.read(paused, "parts") == {:error, {:invalid_child_status, "paused"}}
  end
end
