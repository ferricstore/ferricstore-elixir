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
