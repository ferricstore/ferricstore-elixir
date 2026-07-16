defmodule FerricStore.Flow.Options do
  @moduledoc false

  alias FerricStore.Flow.Options.{
    Admission,
    CollectionValidator,
    MapPreparer,
    RetryPreparer,
    ScalarValuePreparer,
    ValuePreparer,
    ValueValidator
  }

  alias FerricStore.RequestContext
  alias FerricStore.SDK.Native.PreparedRequests

  @transport_options [:timeout, :call_timeout, :lane_id, :idempotent]

  def validate(operation, opts) do
    case prepare(operation, opts) do
      {:ok, _prepared} -> :ok
      {:error, _reason} = error -> error
    end
  end

  @spec prepare(atom(), term()) :: {:ok, keyword()} | {:error, term()}
  def prepare(operation, opts) do
    with :ok <- Admission.validate(operation, opts),
         :ok <- ValueValidator.validate(operation, opts),
         :ok <- CollectionValidator.validate(operation, opts),
         {:ok, opts} <- MapPreparer.prepare(operation, opts) do
      RetryPreparer.prepare(operation, opts)
    end
  end

  @spec prepare_request(atom(), term()) ::
          {:ok, keyword(), RequestContext.t()} | {:error, term()}
  def prepare_request(operation, opts) do
    with :ok <- Admission.validate(operation, opts),
         :ok <- ValueValidator.validate(operation, opts),
         {:ok, context} <- PreparedRequests.prepare(opts, consumed_options(opts)),
         :ok <- CollectionValidator.validate(operation, opts, RequestContext.budget(context)),
         {:ok, opts} <- MapPreparer.prepare(operation, opts, RequestContext.budget(context)),
         {:ok, opts} <- ValuePreparer.prepare(opts, RequestContext.budget(context)),
         {:ok, opts} <-
           ScalarValuePreparer.prepare(operation, opts, RequestContext.budget(context)),
         {:ok, opts} <- RetryPreparer.prepare(operation, opts, RequestContext.budget(context)) do
      {:ok, opts, context}
    end
  end

  @spec validate_noop_request(atom(), term()) :: :ok | {:error, term()}
  def validate_noop_request(operation, opts) do
    with :ok <- Admission.validate_noop(operation, opts),
         :ok <- ValueValidator.validate(operation, opts),
         {:ok, context} <- PreparedRequests.prepare(opts, consumed_options(opts)),
         :ok <- CollectionValidator.validate(operation, opts, RequestContext.budget(context)),
         {:ok, opts} <- MapPreparer.prepare(operation, opts, RequestContext.budget(context)),
         {:ok, _opts} <- RetryPreparer.prepare(operation, opts, RequestContext.budget(context)) do
      :ok
    end
  end

  @spec merge(atom(), keyword(), term()) :: {:ok, keyword()} | {:error, term()}
  def merge(operation, defaults, opts) when is_list(defaults) do
    with :ok <- Admission.validate_list(operation, opts) do
      {:ok, Keyword.merge(defaults, opts)}
    end
  end

  defp consumed_options(opts), do: Keyword.keys(opts) -- @transport_options
end
