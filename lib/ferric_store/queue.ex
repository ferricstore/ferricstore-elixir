defmodule FerricStore.Queue do
  @moduledoc """
  Queue-style API built on FerricFlow states.
  """

  alias FerricStore.FailureFormatter
  alias FerricStore.Flow
  alias FerricStore.Flow.ConsumerConfig
  alias FerricStore.Flow.Options
  alias FerricStore.Queue.BatchRunner
  alias FerricStore.Result
  alias FerricStore.Types

  defstruct [
    :client,
    :type,
    state: "queued",
    worker: "elixir-worker",
    lease_ms: 30_000,
    codec: FerricStore.Codec.Raw
  ]

  @config_defaults [
    state: "queued",
    worker: "elixir-worker",
    lease_ms: 30_000,
    codec: FerricStore.Codec.Raw
  ]

  def new(client, type, opts \\ []) do
    config = ConsumerConfig.validate!(client, type, opts, @config_defaults)
    struct!(__MODULE__, [client: client, type: type] ++ config)
  end

  def enqueue(%__MODULE__{} = queue, id, opts \\ []) do
    with_flow_options(
      :create,
      [type: queue.type, state: queue.state, codec: queue.codec],
      opts,
      &Flow.enqueue(queue.client, id, &1)
    )
  end

  def claim(%__MODULE__{} = queue, opts \\ []) do
    with_flow_options(
      :claim_due,
      [
        state: queue.state,
        worker: queue.worker,
        lease_ms: queue.lease_ms,
        include_record: true,
        payload: true,
        codec: queue.codec
      ],
      opts,
      &Flow.claim_due(queue.client, queue.type, &1)
    )
  end

  def complete(%__MODULE__{} = queue, job, opts \\ []) do
    with_flow_options(
      :complete,
      [
        partition_key: Types.get(job, :partition_key),
        lease_token: Types.get(job, :lease_token),
        fencing_token: Types.get(job, :fencing_token),
        codec: queue.codec
      ],
      opts,
      &Flow.complete(queue.client, Types.get(job, :id), &1)
    )
  end

  def fail(%__MODULE__{} = queue, job, opts \\ []) do
    with_flow_options(
      :fail,
      [
        partition_key: Types.get(job, :partition_key),
        lease_token: Types.get(job, :lease_token),
        fencing_token: Types.get(job, :fencing_token),
        codec: queue.codec
      ],
      opts,
      &Flow.fail(queue.client, Types.get(job, :id), &1)
    )
  end

  def run_once(%__MODULE__{} = queue, handler, opts \\ []) when is_function(handler, 1) do
    max_concurrency = BatchRunner.max_concurrency!(opts)
    claim_opts = opts |> Keyword.delete(:max_concurrency) |> Keyword.put_new(:limit, 1)

    case claim(queue, claim_opts) do
      {:error, _reason} = error ->
        error

      jobs when is_list(jobs) ->
        terminal_opts = Keyword.take(opts, [:timeout, :call_timeout, :lane_id])

        BatchRunner.map(
          jobs,
          &settle_job(queue, &1, handler, terminal_opts),
          &settle_task_exit(queue, &1, &2, terminal_opts),
          max_concurrency
        )

      other ->
        {:error, {:invalid_claim_response, other}}
    end
  end

  defp settle_job(queue, job, handler, terminal_opts) do
    case invoke_handler(handler, job) do
      {:handler_error, reason} ->
        fail(queue, job, Keyword.put(terminal_opts, :error, reason))

      {:ok, {:error, reason}} ->
        fail(queue, job, Keyword.put(terminal_opts, :error, reason))

      {:ok, result} ->
        complete(queue, job, Keyword.put(terminal_opts, :result, result))
    end
  end

  defp invoke_handler(handler, job) do
    {:ok, handler.(job)}
  rescue
    exception ->
      message = FailureFormatter.exception_message(exception, "unrenderable exception")
      {:handler_error, "handler exception: " <> message}
  catch
    kind, reason ->
      rendered = FailureFormatter.inspect_term(reason)
      {:handler_error, "handler #{kind}: " <> rendered}
  end

  defp settle_task_exit(queue, job, reason, terminal_opts) do
    message = "handler task exit: " <> task_exit_reason(reason)
    fail(queue, job, Keyword.put(terminal_opts, :error, message))
  end

  defp task_exit_reason(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp task_exit_reason(reason), do: FailureFormatter.inspect_term(reason)

  defp with_flow_options(operation, defaults, opts, function) do
    case Options.merge(operation, defaults, opts) do
      {:ok, merged} -> function.(merged)
      {:error, reason} -> Result.error(reason)
    end
  end
end
