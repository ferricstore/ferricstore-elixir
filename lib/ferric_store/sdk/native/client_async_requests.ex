defmodule FerricStore.SDK.Native.ClientAsyncRequests do
  @moduledoc false

  alias FerricStore.{
    AsyncDelivery,
    AsyncRequest,
    Protocol,
    RequestContext,
    RequestOptions,
    RouteKey
  }

  alias FerricStore.Protocol.Opcodes
  alias FerricStore.SDK.Native.{ClientRequestAdmission, CoordinatorCall}

  @default_timeout 5_000
  @control_request_option_keys [:timeout, :call_timeout, :idempotent, :lane_id, :endpoint]
  @routed_request_option_keys [:timeout, :call_timeout, :idempotent]

  @spec request(pid(), non_neg_integer() | atom() | binary(), term(), keyword()) :: reference()
  def request(client, opcode, payload, opts) do
    ref = AsyncDelivery.new()
    payload = Protocol.payload_or_empty(payload)

    result =
      with :ok <- ClientRequestAdmission.validate_external_payload(payload),
           {:ok, opcode} <- Opcodes.fetch(opcode),
           {:ok, context} <- request_context(opts, @control_request_option_keys),
           {:ok, context} <- ClientRequestAdmission.prepare_context(opcode, payload, context) do
        submit(client, {:async_request, self(), ref, opcode, payload, context}, context)
      end

    finish(ref, result)
  end

  @spec request_context(
          pid(),
          non_neg_integer() | atom() | binary(),
          term(),
          RequestContext.t()
        ) :: reference()
  def request_context(client, opcode, payload, %RequestContext{} = context) do
    ref = AsyncDelivery.new()
    payload = Protocol.payload_or_empty(payload)

    result =
      with :ok <- validate_context_options(context, @control_request_option_keys),
           :ok <- ClientRequestAdmission.validate_external_payload(payload),
           {:ok, opcode} <- Opcodes.fetch(opcode),
           {:ok, context} <- ClientRequestAdmission.prepare_context(opcode, payload, context) do
        submit(client, {:async_request, self(), ref, opcode, payload, context}, context)
      end

    finish(ref, result)
  end

  @spec request_by_key(
          pid(),
          non_neg_integer() | atom() | binary(),
          binary(),
          term(),
          keyword()
        ) :: reference()
  def request_by_key(client, opcode, key, payload, opts) do
    ref = AsyncDelivery.new()
    payload = Protocol.payload_or_empty(payload)

    result =
      with :ok <- ClientRequestAdmission.validate_external_payload(payload),
           {:ok, opcode} <- Opcodes.fetch(opcode),
           {:ok, ^key} <- RouteKey.validate(key),
           {:ok, context} <- request_context(opts, @routed_request_option_keys),
           {:ok, context} <- ClientRequestAdmission.prepare_context(opcode, payload, context) do
        submit_command(client, ref, opcode, key, payload, context)
      end

    finish(ref, result)
  end

  @spec request_by_key_context(
          pid(),
          non_neg_integer() | atom() | binary(),
          binary(),
          term(),
          RequestContext.t()
        ) :: reference()
  def request_by_key_context(client, opcode, key, payload, %RequestContext{} = context) do
    ref = AsyncDelivery.new()
    payload = Protocol.payload_or_empty(payload)

    result =
      with :ok <- validate_context_options(context, @routed_request_option_keys),
           :ok <- ClientRequestAdmission.validate_external_payload(payload),
           {:ok, opcode} <- Opcodes.fetch(opcode),
           {:ok, ^key} <- RouteKey.validate(key),
           {:ok, context} <- ClientRequestAdmission.prepare_context(opcode, payload, context) do
        submit_command(client, ref, opcode, key, payload, context)
      end

    finish(ref, result)
  end

  defp submit_command(client, ref, opcode, key, payload, context) do
    submit(client, {:async_command, self(), ref, opcode, key, payload, context}, context)
  end

  defp request_context(opts, supported_options),
    do: ClientRequestAdmission.context(opts, @default_timeout, supported_options)

  defp validate_context_options(context, supported_options) do
    case RequestOptions.validate_supported(RequestContext.options(context), supported_options) do
      :ok -> :ok
      {:error, {key, value}} -> {:error, {:invalid_request_option, key, value}}
    end
  end

  defp submit(client, message, context) do
    CoordinatorCall.submit_async(
      client,
      message,
      RequestContext.call_timeout(context, @default_timeout)
    )
  end

  defp finish(ref, {:error, reason}) do
    AsyncDelivery.deliver(ref, AsyncRequest, {:error, reason})
    ref
  end

  defp finish(ref, _result), do: ref
end
