defmodule FerricStore.Flow.ChildrenIntegrationTest do
  use ExUnit.Case, async: false

  alias FerricStore.Flow.{ChildGroupResults, ChildResult}
  alias FerricStore.Workflow

  @moduletag :integration
  @url System.get_env("FERRICSTORE_TEST_URL", "ferric://127.0.0.1:6388")
  @server_version System.get_env("FERRICSTORE_TEST_SERVER_VERSION")

  # Servers before 0.11.19 never make a parent claimable again once its child
  # group resolves, so fan-in cannot finish there.
  if @server_version && Version.compare(@server_version, "0.11.19") == :lt do
    @moduletag skip: "fan-in needs FerricStore 0.11.19 or newer"
  end

  if String.starts_with?(@url, ["http://", "https://"]) do
    @moduletag skip: "covered over the native protocol"
  end

  setup do
    {:ok, client} = FerricStore.start_link(url: @url)
    on_exit(fn -> FerricStore.close(client) end)
    {:ok, client: client}
  end

  test "a parent waits for its children and reads their results", %{client: client} do
    suffix = Integer.to_string(System.unique_integer([:positive]))
    codec = FerricStore.Codec.Term

    parent =
      Workflow.new(client, "ex-children-parent-" <> suffix,
        initial_state: "dispatch",
        codec: codec
      )

    child =
      Workflow.new(client, "ex-children-child-" <> suffix, initial_state: "queued", codec: codec)

    id = "ex-children:" <> suffix

    Workflow.start(parent, id, payload: %{report: id})
    [job] = Workflow.claim(parent, "dispatch", limit: 1)

    Workflow.wait_for_children(
      parent,
      job,
      [
        %{id: id <> ":ok", type: child.type, payload: %{rows: 7}},
        %{id: id <> ":bad", type: child.type, payload: %{rows: -1}}
      ],
      group_id: "items",
      wait_state: "waiting",
      success: "joined",
      failure: "child_failed",
      on_child_failed: :ignore
    )

    assert Workflow.claim(parent, "dispatch", limit: 1) == []

    for claimed <- Workflow.claim(child, "queued", limit: 10) do
      credentials = [
        partition_key: claimed["partition_key"],
        lease_token: claimed["lease_token"],
        fencing_token: claimed["fencing_token"]
      ]

      case claimed["payload"] do
        %{rows: rows} when rows >= 0 ->
          Workflow.complete(child, claimed["id"], [result: %{rows: rows}] ++ credentials)

        _invalid ->
          Workflow.fail(child, claimed["id"], [error: %{reason: "bad input"}] ++ credentials)
      end
    end

    [joined] = Workflow.claim(parent, "joined", limit: 1)

    assert %ChildGroupResults{
             group_id: "items",
             wait: "all",
             resolved: "success",
             summary: %{total: 2, completed: 1, failed: 1, cancelled: 0},
             children: [
               %ChildResult{status: "failed", error: %{reason: "bad input"}} = bad,
               %ChildResult{status: "completed", result: %{rows: 7}} = ok
             ]
           } = Workflow.child_results(parent, joined, group_id: "items")

    assert bad.id == id <> ":bad"
    assert ok.id == id <> ":ok"
    assert Workflow.child_results(parent, joined, group_id: "absent") == nil
  end
end
