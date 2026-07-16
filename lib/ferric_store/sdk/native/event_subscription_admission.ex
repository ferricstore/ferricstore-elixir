defmodule FerricStore.SDK.Native.EventSubscriptionAdmission do
  @moduledoc false

  alias FerricStore.{RequestContext, RequestOptions}
  alias FerricStore.SDK.Native.EventFilterValidator

  @option_keys [:timeout, :call_timeout, :idempotent, :endpoint, :subscriber]

  @spec prepare(term(), term(), timeout()) ::
          {:ok, pid(), RequestContext.t()} | {:error, term()}
  def prepare(events, opts, default_timeout) do
    with :ok <- validate_options(opts),
         {:ok, subscriber} <- validate_subscriber(Keyword.get(opts, :subscriber, self())),
         context = RequestContext.new(Keyword.delete(opts, :subscriber), default_timeout),
         :ok <- EventFilterValidator.validate(events, RequestContext.budget(context)) do
      {:ok, subscriber, context}
    end
  end

  defp validate_options(opts) do
    case RequestOptions.validate_supported(opts, @option_keys) do
      :ok -> :ok
      {:error, {key, value}} -> {:error, {:invalid_request_option, key, value}}
    end
  end

  defp validate_subscriber(subscriber) when is_pid(subscriber), do: {:ok, subscriber}
  defp validate_subscriber(subscriber), do: {:error, {:invalid_event_subscriber, subscriber}}
end
