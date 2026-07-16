defmodule FerricStore.Flow.LifecycleCommands do
  @moduledoc false

  alias FerricStore.Flow.{ArgumentValidator, CommandRuntime, Payload, RequestRuntime}
  alias FerricStore.{Protocol, Result}

  def create(client, id, opts),
    do: request(client, :create, :flow_create, id, opts, &Payload.create_payload(id, &1))

  def enqueue(client, id, opts), do: create(client, id, opts)

  def transition(client, id, opts),
    do:
      request(
        client,
        :transition,
        :flow_transition,
        id,
        opts,
        &Payload.transition_payload(id, &1)
      )

  def complete(client, id, opts),
    do: request(client, :complete, :flow_complete, id, opts, &Payload.complete_payload(id, &1))

  def retry(client, id, opts),
    do: request(client, :retry, :flow_retry, id, opts, &Payload.retry_payload(id, &1))

  def fail(client, id, opts),
    do: request(client, :fail, :flow_fail, id, opts, &Payload.fail_payload(id, &1))

  def cancel(client, id, opts),
    do: request(client, :cancel, :flow_cancel, id, opts, &Payload.cancel_payload(id, &1))

  def signal(client, id, opts),
    do: request(client, :signal, :flow_signal, id, opts, &Payload.signal_payload(id, &1))

  defp request(client, operation, opcode, id, opts, payload_builder) do
    case ArgumentValidator.validate(operation, :id, id) do
      :ok -> execute(client, operation, opcode, opts, payload_builder)
      {:error, reason} -> Result.error(reason)
    end
  end

  defp execute(client, operation, opcode, opts, payload_builder) do
    CommandRuntime.with_options(operation, opts, fn opts, context ->
      RequestRuntime.request(
        client,
        Protocol.opcode(opcode),
        payload_builder.(opts),
        opts,
        context
      )
    end)
  end
end
