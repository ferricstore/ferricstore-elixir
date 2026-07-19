defmodule FerricStore.SDK.Native.ClientRequestAdmission do
  @moduledoc false

  alias FerricStore.{BoundedList, RequestContext, RequestLimits, RequestOptions}
  alias FerricStore.Protocol.{CommandName, PipelineRequest, PreparedMap}
  alias FerricStore.SDK.Native.{ClientOptions, RequestRetrySafety}

  @max_batch_items RequestLimits.max_batch_items()
  @max_command_items RequestLimits.max_command_items()

  @spec validate_client_options(keyword()) :: :ok | {:error, term()}
  def validate_client_options(opts) do
    case ClientOptions.validate(opts) do
      :ok -> :ok
      {:error, {key, value}} -> {:error, {:invalid_client_option, key, value}}
    end
  end

  @spec context(keyword(), timeout(), [atom()]) :: {:ok, RequestContext.t()} | {:error, term()}
  def context(opts, default_timeout, supported_options) do
    context(opts, default_timeout, supported_options, [])
  end

  @spec context(keyword(), timeout(), [atom()], [atom()]) ::
          {:ok, RequestContext.t()} | {:error, term()}
  def context(opts, default_timeout, supported_options, consumed_options) do
    case RequestOptions.validate_supported(opts, supported_options) do
      :ok -> {:ok, RequestContext.new(Keyword.drop(opts, consumed_options), default_timeout)}
      {:error, {key, value}} -> {:error, {:invalid_request_option, key, value}}
    end
  end

  @spec prepare_context(non_neg_integer(), term(), RequestContext.t()) ::
          {:ok, RequestContext.t()} | {:error, term()}
  def prepare_context(opcode, payload, %RequestContext{} = context) do
    with :ok <- RequestContext.ensure_active(context),
         {:ok, _options, item_count} <-
           RequestLimits.prepare(
             opcode,
             payload,
             RequestContext.options(context),
             RequestContext.budget(context)
           ),
         :ok <- RequestContext.ensure_active(context) do
      context = RequestContext.with_batch_item_count(context, item_count)
      {:ok, RequestRetrySafety.classify(opcode, payload, context)}
    end
  end

  @spec normalize_command(term()) :: {:ok, binary()} | {:error, {:invalid_command, map()}}
  def normalize_command(command) do
    case CommandName.normalize(command) do
      {:ok, normalized} -> {:ok, normalized}
      {:error, reason} -> {:error, {:invalid_command, %{reason: reason, value: command}}}
    end
  end

  @spec admit_command_args(term(), RequestContext.t()) :: :ok | {:error, term()}
  def admit_command_args(args, %RequestContext{}) when not is_list(args),
    do: {:error, {:invalid_command_args, :expected_list}}

  def admit_command_args(args, %RequestContext{} = context) do
    case BoundedList.count(args, @max_command_items, RequestContext.budget(context)) do
      {:ok, _count} -> :ok
      {:error, {:limit_exceeded, observed}} -> command_too_large(observed)
      {:error, :improper_list} -> {:error, {:invalid_command_args, :improper_list}}
      {:error, :timeout} = error -> error
    end
  end

  @spec count_batch_items(list(), RequestContext.t()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def count_batch_items(items, %RequestContext{} = context) do
    case BoundedList.count(items, @max_batch_items, RequestContext.budget(context)) do
      {:ok, item_count} -> {:ok, item_count}
      {:error, {:limit_exceeded, observed}} -> batch_too_large(observed)
      {:error, :improper_list} -> {:error, {:invalid_batch_items, :improper_list}}
      {:error, :timeout} = error -> error
    end
  end

  @spec validate_external_payload(term()) :: :ok | {:error, term()}
  def validate_external_payload(%PipelineRequest{}),
    do: {:error, {:invalid_request_payload, %{reason: :reserved_pipeline_envelope}}}

  def validate_external_payload(%PreparedMap{}),
    do: {:error, {:invalid_request_payload, %{reason: :reserved_prepared_map}}}

  def validate_external_payload({:custom_payload, _body, {:batch_items, _count}}),
    do: {:error, {:invalid_request_payload, %{reason: :reserved_batch_envelope}}}

  def validate_external_payload(_payload), do: :ok

  defp command_too_large(observed),
    do: {:error, {:command_too_large, %{items: observed, limit: @max_command_items}}}

  defp batch_too_large(observed),
    do: {:error, {:batch_too_large, %{items: observed, limit: @max_batch_items}}}
end
