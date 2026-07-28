# Workflow and Queue APIs

FerricFlow has two common usage styles in this SDK.

| Style | Use when |
| --- | --- |
| `FerricStore.Queue` | You want durable queued work: enqueue, claim, handle, complete/fail. |
| `FerricStore.Workflow` | You want one durable record moving through explicit business states. |

Both use FerricFlow underneath. Neither replays Elixir code. Each handler should
finish with one durable Flow mutation.

## Queue

```elixir
queue = FerricStore.Queue.new(client, "email", worker: "email-worker")

FerricStore.Queue.enqueue(queue, "email-1",
  payload: "small payload",
  attributes: %{tenant: "acme"},
  values: %{template: "welcome template"}
)

FerricStore.Queue.run_once(queue, fn job ->
  provider_send_email(job["id"])
  "sent"
end)
```

`run_once/3` claims jobs with `FLOW.CLAIM_DUE` and then completes or fails each
job.

For production workers, wrap `run_once/3` in a supervised process:

```elixir
defmodule EmailWorker do
  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  def init(opts) do
    queue = Keyword.fetch!(opts, :queue)
    Process.send_after(self(), :tick, 0)
    {:ok, queue}
  end

  def handle_info(:tick, queue) do
    FerricStore.Queue.run_once(queue, fn job ->
      send_email(job)
      "sent"
    end, limit: 100)

    Process.send_after(self(), :tick, 10)
    {:noreply, queue}
  end
end
```

## Workflow

```elixir
workflow = FerricStore.Workflow.new(client, "payment", initial_state: "created")

FerricStore.Workflow.start(workflow, "payment-1",
  payload: "payment request",
  attributes: %{tenant: "acme"}
)
```

Claim a state:

```elixir
[job | _] = FerricStore.Workflow.claim(workflow, "created",
  worker: "payment-worker",
  limit: 1
)
```

Transition a claimed job:

```elixir
FerricStore.Workflow.transition(workflow, job["id"], "running", "charged",
  partition_key: job["partition_key"],
  lease_token: job["lease_token"],
  fencing_token: job["fencing_token"],
  payload: "charge result"
)
```

Complete a claimed job:

```elixir
FerricStore.Workflow.complete(workflow, job["id"],
  partition_key: job["partition_key"],
  lease_token: job["lease_token"],
  fencing_token: job["fencing_token"],
  result: "ok"
)
```

## Important claim semantics

When `claim_due` succeeds, FerricStore leases the flow and moves current state to
`running`. The originally claimed state is tracked separately. Therefore:

- Claim from business state: `state: "created"`.
- Transition claimed work from `"running"` to next business state.
- Always pass `partition_key`, `lease_token`, and `fencing_token` from the job.

## Retry, fail, cancel

```elixir
FerricStore.Flow.retry(client, job["id"],
  partition_key: job["partition_key"],
  lease_token: job["lease_token"],
  fencing_token: job["fencing_token"],
  error: "temporary provider failure",
  run_at_ms: System.system_time(:millisecond) + 5_000
)

FerricStore.Flow.fail(client, job["id"],
  partition_key: job["partition_key"],
  lease_token: job["lease_token"],
  fencing_token: job["fencing_token"],
  error: "permanent failure"
)
```

`cancel/3` is an operator-style command. It requires the current fencing token
and optional reason, not a lease token:

```elixir
record = FerricStore.Flow.get(client, "flow-1")

FerricStore.Flow.cancel(client, "flow-1",
  partition_key: record["partition_key"],
  fencing_token: record["fencing_token"],
  reason: "operator cancelled"
)
```

## Durable interval schedules

Use the topology-aware `FerricStore.SDK.Flow` wrapper for native schedule
commands. Interval schedules use bounded `fire_once` catch-up by default, and
it is the only catch-up policy accepted for intervals. When due execution
resumes at least one full interval late, FerricStore creates one recovery target
instead of replaying every missed occurrence. Other schedule kinds reject a
catch-up policy.

```elixir
now_ms = System.system_time(:millisecond)

{:ok, schedule} =
  FerricStore.SDK.Flow.schedule_create(client, %{
    id: "hourly-billing",
    kind: "interval",
    every_ms: 60 * 60 * 1_000,
    start_at_ms: now_ms + 60_000,
    catchup_policy: "fire_once",
    overlap_policy: "queue_after_previous",
    target: %{id_prefix: "billing-run", type: "billing"}
  })
```

`catchup_policy` and `overlap_policy` solve different problems:

- `catchup_policy: "fire_once"` coalesces elapsed interval occurrences after
  scheduler downtime or delayed polling.
- `overlap_policy: "queue_after_previous"` holds at most one due occurrence
  while the previous target remains active.

The built-in server scheduler normally owns due execution. Call
`schedule_fire_due/3` only for tests, administrative recovery, or a deployment
that deliberately disables the built-in runner and supplies one custom runner.
Such a runner must use a stable worker identity:

```elixir
{:ok, result} =
  FerricStore.SDK.Flow.schedule_fire_due(client, %{
    worker: "scheduler-a",
    now_ms: System.system_time(:millisecond),
    limit: 100
  })

result["fired"]
result["coalesced"]
```

Each `errors` entry corresponds to a claimed schedule. If a later claim wave
fails after earlier outcomes completed, `claim_error` reports that batch-level
failure separately; it is absent when all claim waves succeed.

The schedule returned by `schedule_create/3`, `schedule_get/3`, and
`schedule_list/3` includes the complete recurrence configuration:
`created_at_ms`, `every_ms`, `cron`, `timezone`, `overlap_policy`, and
`overlap_retry_ms`. It also includes `catchup_policy`, cumulative
`coalesced_count`, `last_catchup_at_ms`, and `last_coalesced_count`.
Non-applicable recurrence fields are `nil`, not omitted. `fire_count` counts
targets actually created, not coalesced occurrences. Catch-up is constant-time
even after long downtime.

If persisted recurrence state cannot be planned, the schedule becomes
`"failed"` with `end_reason: "planning_failed"`; `last_planning_error` contains
the actionable parser or calendar error. No target is created for that failed
occurrence. `schedule_delete/3` returns `{:ok, "OK"}`.

Recurring targets reject a fixed `id`. Set `id_prefix` to choose their
generated prefix, or omit it to use the schedule ID. A returned schedule may
briefly have state `"running"` while the server owns a due-execution lease.
Bounded catch-up is interval-only; overdue cron schedules advance one matching
occurrence per successful automatic fire.
