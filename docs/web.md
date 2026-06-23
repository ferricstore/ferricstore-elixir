# Web and Serverless Usage

The production shape is:

```text
web server / serverless function = client that starts work
worker service / pod / VM = long-running worker that claims and completes work
both agree on Flow type/state
```

## Shared client module

```elixir
defmodule MyApp.Ferric do
  use GenServer

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def client do
    GenServer.call(__MODULE__, :client)
  end

  def init([]) do
    {:ok, client} = FerricStore.start_link(url: ferric_url())
    {:ok, client}
  end

  def handle_call(:client, _from, client), do: {:reply, client, client}

  def terminate(_reason, client), do: FerricStore.close(client)

  defp ferric_url, do: Application.fetch_env!(:my_app, :ferricstore_url)
end
```

Add it to your supervision tree:

```elixir
children = [
  {MyApp.Ferric, []}
]
```

## Phoenix producer

```elixir
defmodule MyAppWeb.EmailController do
  use MyAppWeb, :controller

  def create(conn, params) do
    flow_id = "email:#{params["id"]}"

    queue = FerricStore.Queue.new(MyApp.Ferric.client(), "email")

    FerricStore.Queue.enqueue(queue, flow_id,
      payload: Jason.encode!(params),
      attributes: %{tenant: params["tenant"]}
    )

    json(conn, %{id: flow_id, status: "queued"})
  end
end
```

Request handlers should enqueue/start work and return. Do not run a normal worker
loop inside a request.

## Worker service

```elixir
defmodule MyApp.EmailWorker do
  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  def init(_opts) do
    queue = FerricStore.Queue.new(MyApp.Ferric.client(), "email", worker: "email-worker")
    Process.send_after(self(), :tick, 0)
    {:ok, queue}
  end

  def handle_info(:tick, queue) do
    FerricStore.Queue.run_once(queue, fn job ->
      payload = Jason.decode!(job["payload"] || "{}")
      MyApp.EmailProvider.send(payload)
      "sent"
    end, limit: 100)

    Process.send_after(self(), :tick, 10)
    {:noreply, queue}
  end
end
```

Run this as a separate supervised service or node.

## Serverless producer

A serverless handler should only create work:

```elixir
def handler(event, _context) do
  {:ok, client} = FerricStore.start_link(url: System.fetch_env!("FERRICSTORE_URL"))
  queue = FerricStore.Queue.new(client, "email")

  flow_id = "email:#{event["id"]}"
  FerricStore.Queue.enqueue(queue, flow_id, payload: Jason.encode!(event))

  FerricStore.close(client)
  %{id: flow_id, status: "queued"}
end
```

For high-volume serverless producers, reuse clients where the platform allows
warm process state.
