defmodule FerricStore.RequestContext do
  @moduledoc false

  alias FerricStore.{DeadlineBudget, RequestOptions}

  @internal_options [:__batch_item_count__, :__client_deadline__]

  @enforce_keys [:options, :deadline]
  defstruct [:batch_item_count, :deadline, options: [], automatic_retry: true]

  @type t :: %__MODULE__{
          options: keyword(),
          deadline: DeadlineBudget.t(),
          batch_item_count: non_neg_integer() | nil,
          automatic_retry: boolean()
        }

  @spec new(keyword(), timeout(), non_neg_integer() | nil) :: t()
  def new(options, default_timeout, batch_item_count \\ nil) when is_list(options) do
    options = Keyword.drop(options, @internal_options)
    deadline = options |> RequestOptions.pending_timeout(default_timeout) |> DeadlineBudget.new()

    %__MODULE__{
      options: options,
      deadline: deadline,
      batch_item_count: batch_item_count
    }
  end

  @spec option(t(), atom(), term()) :: term()
  def option(context, key, default \\ nil)

  def option(%__MODULE__{options: options}, key, default),
    do: Keyword.get(options, key, default)

  @spec options(t()) :: keyword()
  def options(%__MODULE__{options: options}), do: options

  @spec budget(t()) :: DeadlineBudget.t()
  def budget(%__MODULE__{deadline: deadline}), do: deadline

  @spec put_option(t(), atom(), term()) :: t()
  def put_option(%__MODULE__{} = context, key, value) when is_atom(key),
    do: %{context | options: Keyword.put(context.options, key, value)}

  @spec with_batch_item_count(t(), non_neg_integer() | nil) :: t()
  def with_batch_item_count(%__MODULE__{} = context, item_count)
      when is_nil(item_count) or (is_integer(item_count) and item_count >= 0),
      do: %{context | batch_item_count: item_count}

  @spec disable_automatic_retry(t()) :: t()
  def disable_automatic_retry(%__MODULE__{} = context),
    do: %{context | automatic_retry: false}

  @spec remaining(t()) :: timeout()
  def remaining(%__MODULE__{deadline: deadline}), do: DeadlineBudget.remaining(deadline)

  @spec expired?(t()) :: boolean()
  def expired?(%__MODULE__{} = context), do: remaining(context) == 0

  @spec ensure_active(t()) :: :ok | {:error, :timeout}
  def ensure_active(%__MODULE__{deadline: deadline}), do: DeadlineBudget.ensure_active(deadline)

  @spec connection_timeout(t(), timeout()) :: timeout()
  def connection_timeout(%__MODULE__{} = context, default_timeout) do
    configured = option(context, :timeout, default_timeout)
    DeadlineBudget.cap(context.deadline, configured)
  end

  @spec call_timeout(t(), timeout()) :: timeout()
  def call_timeout(%__MODULE__{options: options, deadline: deadline}, default_timeout) do
    RequestOptions.call_timeout(options, default_timeout, DeadlineBudget.remaining(deadline))
  end
end
