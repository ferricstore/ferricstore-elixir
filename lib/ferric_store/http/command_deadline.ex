defmodule FerricStore.HTTP.CommandDeadline do
  @moduledoc false

  alias FerricStore.{DeadlineBudget, RequestContext, Timeout}
  alias FerricStore.HTTP.CommandDeadline.BlockingBudget
  alias FerricStore.HTTP.{Error, Options}

  @max_finite_timeout Timeout.max_finite()

  @spec new(Options.t(), term(), timeout()) :: DeadlineBudget.t()
  def new(%Options{} = config, message, fallback) do
    config
    |> timeout(message, fallback)
    |> DeadlineBudget.new()
  end

  @spec timeout(Options.t(), term(), timeout()) :: timeout()
  def timeout(%Options{} = config, message, fallback) do
    context = message_context(message)

    if explicit_deadline?(context) do
      message_timeout(context, fallback)
    else
      base = minimum(message_timeout(context, fallback), config.timeout)
      apply_blocking_budget(base, BlockingBudget.for_message(message))
    end
  end

  @spec ensure_active(DeadlineBudget.t()) :: :ok | {:error, Error.t()}
  def ensure_active(%DeadlineBudget{} = budget) do
    case DeadlineBudget.ensure_active(budget) do
      :ok -> :ok
      {:error, :timeout} -> Error.timeout()
    end
  end

  defp message_timeout(%RequestContext{} = context, fallback),
    do: minimum(RequestContext.remaining(context), fallback)

  defp message_timeout(_missing, fallback), do: fallback

  defp message_context({:request, _opcode, _payload, context}), do: context
  defp message_context({:command, _opcode, _key, _payload, context}), do: context

  defp message_context({:command_items, _opcode, _items, _count, _key_fun, _builder, context}),
    do: context

  defp message_context({:async_request, _owner, _ref, _opcode, _payload, context}), do: context

  defp message_context({:async_command, _owner, _ref, _opcode, _key, _payload, context}),
    do: context

  defp message_context(_message), do: nil

  defp explicit_deadline?(%RequestContext{} = context) do
    options = RequestContext.options(context)
    Keyword.has_key?(options, :timeout) or Keyword.has_key?(options, :call_timeout)
  end

  defp explicit_deadline?(_missing), do: false

  defp apply_blocking_budget(0, _budget), do: 0
  defp apply_blocking_budget(_base, :disable_default), do: :infinity
  defp apply_blocking_budget(:infinity, _budget), do: :infinity

  defp apply_blocking_budget(base, {:extend, extension}) do
    if extension > @max_finite_timeout - base,
      do: :infinity,
      else: base + extension
  end

  defp minimum(:infinity, second), do: second
  defp minimum(first, :infinity), do: first
  defp minimum(first, second), do: min(first, second)
end
