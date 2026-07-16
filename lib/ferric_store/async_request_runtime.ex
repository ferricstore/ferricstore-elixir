defmodule FerricStore.AsyncRequestRuntime do
  @moduledoc false

  alias FerricStore.{AsyncDelivery, AsyncRequest, DeadlineBudget, Error, Result, Timeout}

  alias FerricStore.SDK.Native.Client, as: NativeClient

  @await_cancel_timeout 10

  @spec await(term(), timeout()) :: term()
  def await(%AsyncRequest{owner: owner}, _timeout) when owner != self() do
    {:error,
     %Error{
       message: "FerricStore async request belongs to another process",
       raw: {:invalid_async_owner, owner}
     }}
  end

  def await(%AsyncRequest{} = request, timeout) do
    with :ok <- validate(request),
         true <- Timeout.valid?(timeout) || {:error, {:invalid_timeout, timeout}} do
      await_result(request, timeout)
    else
      {:error, {:invalid_timeout, invalid}} -> invalid_timeout(invalid)
      {:error, reason} -> invalid_request(reason)
    end
  end

  def await(_request, _timeout), do: invalid_request(:expected_handle)

  @spec yield(term(), timeout()) :: term()
  def yield(%AsyncRequest{owner: owner}, _timeout) when owner != self(), do: nil

  def yield(%AsyncRequest{} = request, timeout) do
    with :ok <- validate(request),
         true <- Timeout.valid?(timeout) || {:error, {:invalid_timeout, timeout}} do
      yield_result(request, timeout)
    else
      {:error, {:invalid_timeout, invalid}} -> invalid_timeout(invalid)
      {:error, reason} -> invalid_request(reason)
    end
  end

  def yield(_request, _timeout), do: invalid_request(:expected_handle)

  @spec cancel(term(), timeout()) :: :ok | {:error, term()}
  def cancel(%AsyncRequest{owner: owner}, _timeout) when owner != self(),
    do: {:error, {:invalid_async_owner, owner}}

  def cancel(%AsyncRequest{} = request, timeout) do
    case validate(request) do
      :ok ->
        result = NativeClient.cancel_async(request.client, request.owner, request.ref, timeout)

        if result == :ok, do: deactivate(request.ref)
        result

      {:error, reason} ->
        {:error, {:invalid_async_request, reason}}
    end
  end

  def cancel(_request, _timeout),
    do: {:error, {:invalid_async_request, :expected_handle}}

  defp await_result(request, timeout) do
    deadline = DeadlineBudget.new(timeout)
    monitor = Process.monitor(request.source)

    receive do
      {AsyncRequest, ref, result} when ref == request.ref ->
        finish_monitor(monitor)
        deactivate(ref)
        Result.unwrap(result)

      {:DOWN, ^monitor, :process, _source, _reason} ->
        terminal_client_result(request.ref)
    after
      DeadlineBudget.remaining(deadline) ->
        finish_monitor(monitor)
        AsyncDelivery.deactivate(request.ref)
        cancel(request, @await_cancel_timeout)
        {:error, %Error{message: "FerricStore async request timed out", raw: :timeout}}
    end
  end

  defp yield_result(request, timeout) do
    deadline = DeadlineBudget.new(timeout)
    monitor = Process.monitor(request.source)

    receive do
      {AsyncRequest, ref, result} when ref == request.ref ->
        finish_monitor(monitor)
        deactivate(ref)
        {:ok, Result.unwrap(result)}

      {:DOWN, ^monitor, :process, _source, _reason} ->
        {:ok, terminal_client_result(request.ref)}
    after
      DeadlineBudget.remaining(deadline) ->
        finish_monitor(monitor)
        nil
    end
  end

  defp terminal_client_result(ref) do
    deactivate(ref)
    Result.error(:client_closed)
  end

  defp deactivate(ref) do
    AsyncDelivery.deactivate(ref)
    AsyncDelivery.drain(ref, AsyncRequest)
  end

  defp finish_monitor(monitor), do: Process.demonitor(monitor, [:flush])

  defp validate(%AsyncRequest{client: client}) when not is_pid(client),
    do: {:error, :invalid_client}

  defp validate(%AsyncRequest{source: source}) when not is_pid(source),
    do: {:error, :invalid_source}

  defp validate(%AsyncRequest{ref: ref}) when not is_reference(ref),
    do: {:error, :invalid_reference}

  defp validate(%AsyncRequest{}), do: :ok

  defp invalid_request(reason) do
    {:error,
     %Error{
       message: "FerricStore async request handle is invalid",
       raw: {:invalid_async_request, reason}
     }}
  end

  defp invalid_timeout(timeout) do
    {:error,
     %Error{
       message: "FerricStore timeout must be :infinity or a portable non-negative timer",
       raw: {:invalid_timeout, timeout}
     }}
  end
end
