defmodule FerricStore.Flow.PolicyCommands do
  @moduledoc false

  alias FerricStore.Flow.{ArgumentValidator, CommandRuntime, PolicyCommand, RequestRuntime}
  alias FerricStore.{Protocol, RequestContext, Result}

  def set(client, type, opts \\ []),
    do: request(client, :policy_set, :flow_policy_set, type, opts, &PolicyCommand.set_payload/3)

  def get(client, type, opts \\ []),
    do: request(client, :policy_get, :flow_policy_get, type, opts, &PolicyCommand.get_payload/3)

  defp request(client, operation, opcode, type, opts, payload_builder) do
    case ArgumentValidator.validate(operation, :type, type) do
      :ok -> execute(client, operation, opcode, type, opts, payload_builder)
      {:error, reason} -> Result.error(reason)
    end
  end

  defp execute(client, operation, opcode, type, opts, payload_builder) do
    CommandRuntime.with_options(operation, opts, fn opts, context ->
      execute_payload(client, opcode, type, opts, context, payload_builder)
    end)
  end

  defp execute_payload(client, opcode, type, opts, context, payload_builder) do
    case payload_builder.(type, policy_options(opts), RequestContext.budget(context)) do
      {:ok, payload} ->
        RequestRuntime.request(client, Protocol.opcode(opcode), payload, opts, context)

      {:error, reason} ->
        Result.error(reason)
    end
  end

  defp policy_options(opts), do: Keyword.drop(opts, [:timeout, :call_timeout, :lane_id])
end
