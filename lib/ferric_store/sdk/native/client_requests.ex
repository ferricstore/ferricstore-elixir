defmodule FerricStore.SDK.Native.ClientRequests do
  @moduledoc false

  alias FerricStore.Protocol
  alias FerricStore.Protocol.Opcodes
  alias FerricStore.RequestContext
  alias FerricStore.RouteKey

  alias FerricStore.SDK.Native.{
    ClientAsyncRequests,
    ClientBatchRequests,
    ClientCommandRequests,
    ClientLifecycleRequests,
    ClientRequestAdmission,
    CoordinatorCall
  }

  @default_timeout 5_000
  @control_request_option_keys [:timeout, :call_timeout, :idempotent, :lane_id, :endpoint]
  @routed_request_option_keys [:timeout, :call_timeout, :idempotent]
  @batch_request_option_keys [
    :timeout,
    :call_timeout,
    :idempotent,
    :max_group_concurrency,
    :require_same_shard
  ]

  defdelegate start_link(opts), to: ClientLifecycleRequests
  defdelegate close(client, timeout), to: ClientLifecycleRequests
  defdelegate cancel_async(client, owner, ref, timeout), to: ClientLifecycleRequests
  defdelegate from_url(url, opts), to: ClientLifecycleRequests
  defdelegate event_subscription(client, action, events, opts), to: ClientLifecycleRequests
  defdelegate await_event(client, timeout), to: ClientLifecycleRequests
  defdelegate topology(client), to: ClientLifecycleRequests
  defdelegate route(client, key), to: ClientLifecycleRequests
  defdelegate refresh_topology(client, timeout), to: ClientLifecycleRequests

  def ping(client, message, opts),
    do: request(client, Opcodes.ping(), %{"message" => message}, opts)

  defdelegate command_exec(client, command, args, opts), to: ClientCommandRequests

  defdelegate command_exec_context(client, command, args, context),
    to: ClientCommandRequests

  @spec request(pid(), non_neg_integer() | atom() | binary(), term(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def request(client, opcode, payload, opts) do
    payload = Protocol.payload_or_empty(payload)

    with :ok <- ClientRequestAdmission.validate_external_payload(payload),
         {:ok, opcode} <- Opcodes.fetch(opcode),
         {:ok, context} <- request_context(opts, @control_request_option_keys) do
      submit_request(client, opcode, payload, context)
    end
  end

  defdelegate async_request(client, opcode, payload, opts),
    to: ClientAsyncRequests,
    as: :request

  defdelegate async_request_context(client, opcode, payload, context),
    to: ClientAsyncRequests,
    as: :request_context

  @spec request_by_key(
          pid(),
          non_neg_integer() | atom() | binary(),
          binary(),
          term(),
          keyword()
        ) :: {:ok, term()} | {:error, term()}
  def request_by_key(client, opcode, key, payload, opts) do
    payload = Protocol.payload_or_empty(payload)

    with :ok <- ClientRequestAdmission.validate_external_payload(payload),
         {:ok, opcode} <- Opcodes.fetch(opcode),
         {:ok, ^key} <- RouteKey.validate(key),
         {:ok, context} <- request_context(opts, @routed_request_option_keys) do
      submit_routed_request(client, opcode, key, payload, context)
    end
  end

  defdelegate async_request_by_key(client, opcode, key, payload, opts),
    to: ClientAsyncRequests,
    as: :request_by_key

  defdelegate async_request_by_key_context(client, opcode, key, payload, context),
    to: ClientAsyncRequests,
    as: :request_by_key_context

  @spec request_by_items(
          pid(),
          non_neg_integer() | atom() | binary(),
          list(),
          (term() -> binary()),
          (list() -> map()),
          keyword()
        ) :: {:ok, [map()]} | {:error, term()}
  def request_by_items(client, opcode, items, key_fun, payload_builder, opts) do
    with :ok <- validate_batch_arguments(items, key_fun, payload_builder),
         {:ok, opcode} <- Opcodes.fetch(opcode),
         {:ok, context} <- request_context(opts, @batch_request_option_keys),
         :ok <- RequestContext.ensure_active(context),
         {:ok, groups, _item_count} <-
           ClientBatchRequests.request_with_count(
             client,
             opcode,
             items,
             key_fun,
             payload_builder,
             context
           ) do
      {:ok, groups}
    end
  end

  defp validate_batch_arguments(items, key_fun, payload_builder) do
    cond do
      not is_list(items) -> {:error, {:invalid_batch_items, :expected_list}}
      not is_function(key_fun, 1) -> {:error, {:invalid_batch_callback, :key_fun}}
      not is_function(payload_builder, 1) -> {:error, {:invalid_batch_callback, :payload_builder}}
      true -> :ok
    end
  end

  defp submit_request(client, opcode, payload, context) do
    with {:ok, context} <- ClientRequestAdmission.prepare_context(opcode, payload, context) do
      CoordinatorCall.submit(client, {:request, opcode, payload, context}, call_timeout(context))
    end
  end

  defp submit_routed_request(client, opcode, key, payload, context) do
    with {:ok, context} <- ClientRequestAdmission.prepare_context(opcode, payload, context) do
      CoordinatorCall.submit(
        client,
        {:command, opcode, key, payload, context},
        call_timeout(context)
      )
    end
  end

  defp request_context(opts, supported_options),
    do: ClientRequestAdmission.context(opts, @default_timeout, supported_options)

  defp call_timeout(context), do: RequestContext.call_timeout(context, @default_timeout)
end
