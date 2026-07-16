defmodule FerricStore.SDK.Native.PipelineRequests do
  @moduledoc false

  alias FerricStore.{AsyncDelivery, AsyncRequest, RequestContext, RequestOptions}
  alias FerricStore.Protocol.{Opcodes, PipelineRequest}
  alias FerricStore.SDK.Native.{CoordinatorCall, PipelineAdmission}

  @default_timeout 5_000
  @max_pipeline_commands 100_000
  @pipeline_option_keys [:request_context, :return]
  @request_option_keys [:timeout, :call_timeout, :idempotent, :lane_id]
  @return_modes [:compact, "compact", :pairs, "pairs"]

  @spec request(pid(), list(), keyword(), keyword()) :: {:ok, term()} | {:error, term()}
  def request(client, commands, pipeline_options, opts)
      when is_list(commands) do
    with :ok <- validate_pipeline_options(pipeline_options),
         {:ok, context} <- request_context(opts),
         :ok <- RequestContext.ensure_active(context),
         {:ok, command_count} <- command_count(commands, context),
         :ok <- RequestContext.ensure_active(context) do
      context = RequestContext.with_batch_item_count(context, command_count)

      payload = %PipelineRequest{
        commands: commands,
        command_count: command_count,
        options: pipeline_options
      }

      CoordinatorCall.submit(
        client,
        {:request, Opcodes.pipeline(), payload, context},
        call_timeout(context)
      )
    end
  end

  def request(_client, commands, _pipeline_options, _opts) when not is_list(commands),
    do: {:error, {:invalid_pipeline, :expected_list}}

  @spec async_request(pid(), list(), keyword(), keyword()) :: reference()
  def async_request(client, commands, pipeline_options, opts)
      when is_list(commands) do
    ref = AsyncDelivery.new()

    result =
      with :ok <- validate_pipeline_options(pipeline_options),
           {:ok, context} <- request_context(opts),
           :ok <- RequestContext.ensure_active(context),
           {:ok, command_count} <- command_count(commands, context),
           :ok <- RequestContext.ensure_active(context) do
        context = RequestContext.with_batch_item_count(context, command_count)

        payload = %PipelineRequest{
          commands: commands,
          command_count: command_count,
          options: pipeline_options
        }

        CoordinatorCall.submit_async(
          client,
          {:async_request, self(), ref, Opcodes.pipeline(), payload, context},
          call_timeout(context)
        )
      end

    if match?({:error, _reason}, result) do
      {:error, reason} = result
      AsyncDelivery.deliver(ref, AsyncRequest, {:error, reason})
    end

    ref
  end

  def async_request(_client, commands, _pipeline_options, _opts) when not is_list(commands) do
    ref = AsyncDelivery.new()
    AsyncDelivery.deliver(ref, AsyncRequest, {:error, {:invalid_pipeline, :expected_list}})
    ref
  end

  defp command_count(commands, context) do
    PipelineAdmission.admit(
      commands,
      @max_pipeline_commands,
      RequestContext.budget(context)
    )
  end

  defp validate_pipeline_options(options) do
    with :ok <- validate_pipeline_option_container(options),
         :ok <- validate_supported_pipeline_options(options) do
      case Keyword.fetch(options, :return) do
        :error -> :ok
        {:ok, mode} when mode in @return_modes -> :ok
        {:ok, mode} -> {:error, {:invalid_pipeline_option, :return, mode}}
      end
    end
  end

  defp validate_supported_pipeline_options(options) do
    case Enum.find(options, fn {key, _value} -> key not in @pipeline_option_keys end) do
      nil -> :ok
      {key, value} -> {:error, {:invalid_pipeline_option, key, value}}
    end
  end

  defp validate_pipeline_option_container(options) do
    case RequestOptions.validate(options) do
      :ok -> :ok
      {:error, {key, value}} -> {:error, {:invalid_pipeline_option, key, value}}
    end
  end

  defp request_context(opts) do
    case RequestOptions.validate_supported(opts, @request_option_keys) do
      :ok -> {:ok, RequestContext.new(opts, @default_timeout)}
      {:error, {key, value}} -> {:error, {:invalid_request_option, key, value}}
    end
  end

  defp call_timeout(context), do: RequestContext.call_timeout(context, @default_timeout)
end
