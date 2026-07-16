defmodule FerricStore.SDK.Native.PreparedRequests do
  @moduledoc false

  alias FerricStore.Protocol
  alias FerricStore.Protocol.Opcodes
  alias FerricStore.RequestContext
  alias FerricStore.RequestLimits
  alias FerricStore.RequestOptions
  alias FerricStore.RouteKey

  alias FerricStore.SDK.Native.{
    ClientRequestAdmission,
    ClientRequests,
    CoordinatorCall
  }

  @default_timeout 5_000
  @control_request_option_keys [:timeout, :call_timeout, :idempotent, :lane_id, :endpoint]
  @routed_request_option_keys [:timeout, :call_timeout, :idempotent]

  @spec prepare_command_context(keyword()) :: {:ok, RequestContext.t()} | {:error, term()}
  def prepare_command_context(opts) do
    with {:ok, context} <-
           ClientRequestAdmission.context(
             opts,
             @default_timeout,
             [:key, :request_context | @control_request_option_keys]
           ),
         :ok <- RequestContext.ensure_active(context),
         do: {:ok, context}
  end

  @spec command_exec(pid(), binary(), list(), RequestContext.t()) ::
          {:ok, term()} | {:error, term()}
  defdelegate command_exec(client, command, args, context),
    to: ClientRequests,
    as: :command_exec_context

  @spec prepare(keyword(), [atom()]) :: {:ok, RequestContext.t()} | {:error, term()}
  def prepare(opts, consumed_options \\ []) do
    supported_options = consumed_options ++ @control_request_option_keys

    with {:ok, context} <-
           ClientRequestAdmission.context(
             opts,
             @default_timeout,
             supported_options,
             consumed_options
           ),
         :ok <- RequestContext.ensure_active(context),
         do: {:ok, context}
  end

  @spec prepare_native(
          non_neg_integer() | atom() | binary(),
          keyword(),
          [atom()]
        ) :: {:ok, non_neg_integer(), RequestContext.t()} | {:error, term()}
  def prepare_native(opcode, opts, consumed_options \\ []) do
    with {:ok, context} <- prepare(opts, consumed_options),
         {:ok, opcode} <- Opcodes.fetch(opcode),
         do: {:ok, opcode, context}
  end

  @spec request(pid(), non_neg_integer() | atom() | binary(), term(), RequestContext.t()) ::
          {:ok, term()} | {:error, term()}
  def request(client, opcode, payload, %RequestContext{} = context) do
    payload = Protocol.payload_or_empty(payload)

    with :ok <- validate_context_options(context, @control_request_option_keys),
         :ok <- ClientRequestAdmission.validate_external_payload(payload),
         {:ok, opcode} <- Opcodes.fetch(opcode),
         {:ok, context} <- ClientRequestAdmission.prepare_context(opcode, payload, context) do
      CoordinatorCall.submit(client, {:request, opcode, payload, context}, call_timeout(context))
    end
  end

  @spec request_by_key(
          pid(),
          non_neg_integer() | atom() | binary(),
          binary(),
          term(),
          RequestContext.t()
        ) :: {:ok, term()} | {:error, term()}
  def request_by_key(client, opcode, key, payload, %RequestContext{} = context) do
    payload = Protocol.payload_or_empty(payload)

    with :ok <- validate_context_options(context, @routed_request_option_keys),
         :ok <- ClientRequestAdmission.validate_external_payload(payload),
         {:ok, opcode} <- Opcodes.fetch(opcode),
         {:ok, ^key} <- RouteKey.validate(key),
         {:ok, context} <- ClientRequestAdmission.prepare_context(opcode, payload, context) do
      CoordinatorCall.submit(
        client,
        {:command, opcode, key, payload, context},
        call_timeout(context)
      )
    end
  end

  @doc false
  @spec async_request(
          pid(),
          non_neg_integer() | atom() | binary(),
          term(),
          RequestContext.t()
        ) :: reference()
  defdelegate async_request(client, opcode, payload, context),
    to: ClientRequests,
    as: :async_request_context

  @doc false
  @spec async_request_by_key(
          pid(),
          non_neg_integer() | atom() | binary(),
          binary(),
          term(),
          RequestContext.t()
        ) :: reference()
  defdelegate async_request_by_key(client, opcode, key, payload, context),
    to: ClientRequests,
    as: :async_request_by_key_context

  @doc false
  @spec request_trusted_batch(
          pid(),
          non_neg_integer() | atom() | binary(),
          term(),
          non_neg_integer(),
          RequestContext.t()
        ) :: {:ok, term()} | {:error, term()}
  def request_trusted_batch(
        client,
        opcode,
        {:custom_payload, body} = payload,
        item_count,
        %RequestContext{} = context
      )
      when (is_binary(body) or is_list(body)) and is_integer(item_count) and item_count >= 0 do
    with :ok <- validate_context_options(context, @control_request_option_keys),
         {:ok, opcode} <- Opcodes.fetch(opcode),
         :ok <- RequestContext.ensure_active(context),
         :ok <- RequestLimits.admit(item_count, RequestLimits.max_batch_items()) do
      context = RequestContext.with_batch_item_count(context, item_count)
      CoordinatorCall.submit(client, {:request, opcode, payload, context}, call_timeout(context))
    end
  end

  def request_trusted_batch(_client, _opcode, payload, item_count, %RequestContext{}),
    do: {:error, {:invalid_trusted_batch, %{payload: payload, item_count: item_count}}}

  defp validate_context_options(context, supported_options) do
    case RequestOptions.validate_supported(RequestContext.options(context), supported_options) do
      :ok -> :ok
      {:error, {key, value}} -> {:error, {:invalid_request_option, key, value}}
    end
  end

  defp call_timeout(context), do: RequestContext.call_timeout(context, @default_timeout)
end
