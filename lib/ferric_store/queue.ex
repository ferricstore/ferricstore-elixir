defmodule FerricStore.Queue do
  @moduledoc """
  Queue-style API built on FerricFlow states.
  """

  alias FerricStore.Flow
  alias FerricStore.Types

  defstruct [
    :client,
    :type,
    state: "queued",
    worker: "elixir-worker",
    lease_ms: 30_000,
    codec: FerricStore.Codec.Raw
  ]

  def new(client, type, opts \\ []) do
    struct(__MODULE__, Keyword.merge([client: client, type: type], opts))
  end

  def enqueue(%__MODULE__{} = queue, id, opts \\ []) do
    Flow.enqueue(
      queue.client,
      id,
      Keyword.merge([type: queue.type, state: queue.state, codec: queue.codec], opts)
    )
  end

  def claim(%__MODULE__{} = queue, opts \\ []) do
    Flow.claim_due(
      queue.client,
      queue.type,
      Keyword.merge([state: queue.state, worker: queue.worker, lease_ms: queue.lease_ms], opts)
    )
  end

  def complete(%__MODULE__{} = queue, job, opts \\ []) do
    Flow.complete(
      queue.client,
      Types.get(job, :id),
      Keyword.merge(
        [
          lease_token: Types.get(job, :lease_token),
          fencing_token: Types.get(job, :fencing_token),
          codec: queue.codec
        ],
        opts
      )
    )
  end

  def fail(%__MODULE__{} = queue, job, opts \\ []) do
    Flow.fail(
      queue.client,
      Types.get(job, :id),
      Keyword.merge(
        [
          lease_token: Types.get(job, :lease_token),
          fencing_token: Types.get(job, :fencing_token),
          codec: queue.codec
        ],
        opts
      )
    )
  end

  def run_once(%__MODULE__{} = queue, handler, opts \\ []) when is_function(handler, 1) do
    queue
    |> claim(Keyword.put_new(opts, :limit, 1))
    |> List.wrap()
    |> Enum.map(fn job ->
      case handler.(job) do
        {:error, reason} -> fail(queue, job, error: reason)
        result -> complete(queue, job, result: result)
      end
    end)
  end
end
