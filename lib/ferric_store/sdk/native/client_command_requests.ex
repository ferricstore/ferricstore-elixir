defmodule FerricStore.SDK.Native.ClientCommandRequests do
  @moduledoc false

  alias FerricStore.Protocol.Opcodes
  alias FerricStore.Protocol.RequestContextCodec
  alias FerricStore.{RequestContext, RouteKey}
  alias FerricStore.SDK.Native.{ClientRequestAdmission, CoordinatorCall}

  @default_timeout 5_000
  @control_request_option_keys [:timeout, :call_timeout, :idempotent, :lane_id, :endpoint]
  @command_request_option_keys [:key, :request_context | @control_request_option_keys]

  def command_exec(client, command, args, opts) do
    with {:ok, command} <- ClientRequestAdmission.normalize_command(command),
         {:ok, context} <-
           ClientRequestAdmission.context(opts, @default_timeout, @command_request_option_keys) do
      execute(client, command, args, context)
    end
  end

  def command_exec_context(client, command, args, %RequestContext{} = context) do
    with {:ok, command} <- ClientRequestAdmission.normalize_command(command) do
      execute(client, command, args, context)
    end
  end

  defp execute(client, command, args, context) do
    with :ok <- RequestContext.ensure_active(context),
         {:ok, route} <- command_route(RequestContext.options(context)),
         :ok <- ClientRequestAdmission.admit_command_args(args, context),
         :ok <- RequestContext.ensure_active(context),
         {:ok, payload} <-
           RequestContextCodec.put_result(
             %{"command" => command, "args" => args},
             RequestContext.options(context)
           ) do
      submit(client, route, Opcodes.command_exec(), payload, context)
    end
  end

  defp submit(client, {:routed, key}, opcode, payload, context) do
    with {:ok, context} <- ClientRequestAdmission.prepare_context(opcode, payload, context) do
      CoordinatorCall.submit(
        client,
        {:command, opcode, key, payload, context},
        call_timeout(context)
      )
    end
  end

  defp submit(client, :control, opcode, payload, context) do
    with {:ok, context} <- ClientRequestAdmission.prepare_context(opcode, payload, context) do
      CoordinatorCall.submit(client, {:request, opcode, payload, context}, call_timeout(context))
    end
  end

  defp command_route(opts) do
    case RouteKey.from_options(opts, [:key]) do
      {:ok, key} ->
        with :ok <- reject_routed_options(opts), do: {:ok, {:routed, key}}

      :none ->
        {:ok, :control}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp reject_routed_options(opts) do
    case Enum.find(opts, fn {key, _value} -> key in [:lane_id, :endpoint] end) do
      nil -> :ok
      {key, value} -> {:error, {:invalid_request_option, key, value}}
    end
  end

  defp call_timeout(context), do: RequestContext.call_timeout(context, @default_timeout)
end
