defmodule FerricStore.Client do
  @moduledoc """
  Public client facade backed by the topology-aware native coordinator.

  This module does not own a socket or implement a second protocol session.
  `FerricStore.start_link/1` and `FerricStore.SDK.start_link/1` return the same
  client type and share one internal connection layer for all transport IO.
  """

  alias FerricStore.{AsyncDelivery, AsyncRequest}
  alias FerricStore.AsyncRequestRuntime
  alias FerricStore.Error
  alias FerricStore.FailureFormatter
  alias FerricStore.NativeRequestRuntime
  alias FerricStore.RequestOptions
  alias FerricStore.Result
  alias FerricStore.RouteKey
  alias FerricStore.SDK.Native.Client, as: NativeClient
  alias FerricStore.SDK.Native.ClientOptions
  alias FerricStore.SDK.Native.ClientSupervisor

  @default_url "ferric://127.0.0.1:6388"
  @default_timeout 5_000
  @pipeline_request_option_keys [:timeout, :call_timeout, :idempotent, :lane_id]
  @pipeline_option_keys [:return, :request_context]
  @pipeline_supported_option_keys Enum.uniq(
                                    @pipeline_option_keys ++ @pipeline_request_option_keys
                                  )

  @spec child_spec(keyword() | binary()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      shutdown: :infinity,
      type: :supervisor
    }
  end

  @spec start_link(keyword() | binary()) :: GenServer.on_start()
  def start_link(opts \\ [])

  def start_link(url) when is_binary(url), do: NativeClient.from_url(url)

  def start_link(opts) do
    with {:ok, {url, opts}} <- ClientOptions.take_url(opts) do
      start_with_url(url, opts)
    end
  end

  @spec connect!(keyword() | binary()) :: pid()
  def connect!(opts \\ []) do
    case start_link(opts) do
      {:ok, client} ->
        client

      {:error, reason} ->
        raise Error,
          message: "connect failed: #{FailureFormatter.inspect_term(reason)}",
          raw: reason
    end
  end

  @spec close(pid(), timeout()) :: :ok | {:error, term()}
  def close(client, timeout \\ @default_timeout), do: NativeClient.close(client, timeout)

  def command(client, command, args \\ [], opts \\ []) do
    case RouteKey.from_options(opts, [:key, :route_key]) do
      {:ok, key} ->
        opts = opts |> request_opts() |> Keyword.put(:key, key)
        client |> NativeClient.command_exec(command, args, opts) |> Result.unwrap()

      :none ->
        client |> NativeClient.command_exec(command, args, request_opts(opts)) |> Result.unwrap()

      {:error, reason} ->
        Result.error(reason)
    end
  end

  def pipeline(client, commands, opts \\ []) do
    case pipeline_options(opts) do
      {:ok, pipeline_options, request_options} ->
        client
        |> NativeClient.pipeline(commands, pipeline_options, request_options)
        |> Result.unwrap()

      {:error, reason} ->
        Result.error(reason)
    end
  end

  def async_pipeline(client, commands, opts \\ []) do
    source = async_source(client)

    case pipeline_options(opts) do
      {:ok, pipeline_options, request_options} ->
        client
        |> NativeClient.async_pipeline(commands, pipeline_options, request_options)
        |> then(&async_handle(client, source, &1))

      {:error, reason} ->
        async_error_handle(client, reason)
    end
  end

  def native(client, opcode, payload, opts \\ []) do
    client |> NativeRequestRuntime.request(opcode, payload, opts) |> Result.unwrap()
  end

  @doc false
  def native_batch(client, opcode, payload, item_count, opts) do
    client
    |> NativeClient.request_trusted_batch(opcode, payload, item_count, request_opts(opts))
    |> Result.unwrap()
  end

  def async_native(client, opcode, payload, opts \\ []) do
    source = async_source(client)

    case NativeRequestRuntime.async_request(client, opcode, payload, opts) do
      {:ok, ref} -> async_handle(client, source, ref)
      {:error, reason} -> async_error_handle(client, reason)
    end
  end

  def await(request, timeout \\ @default_timeout)
  def await(request, timeout), do: AsyncRequestRuntime.await(request, timeout)

  def yield(request, timeout \\ 0)
  def yield(request, timeout), do: AsyncRequestRuntime.yield(request, timeout)

  def cancel_async(request, timeout \\ @default_timeout)
  def cancel_async(request, timeout), do: AsyncRequestRuntime.cancel(request, timeout)

  defp start_with_url(nil, opts) do
    if Keyword.has_key?(opts, :seeds),
      do: NativeClient.start_link(opts),
      else: NativeClient.from_url(@default_url, opts)
  end

  defp start_with_url(url, opts), do: NativeClient.from_url(url, opts)

  defp request_opts(opts), do: Keyword.drop(opts, [:key, :route_key])

  defp pipeline_options(opts) do
    case RequestOptions.validate(opts) do
      :ok -> split_pipeline_options(opts)
      {:error, {key, value}} -> {:error, {:invalid_request_option, key, value}}
    end
  end

  defp split_pipeline_options(opts) do
    case Enum.find(opts, fn {key, _value} -> key not in @pipeline_supported_option_keys end) do
      nil ->
        {:ok, Keyword.take(opts, @pipeline_option_keys),
         Keyword.take(opts, @pipeline_request_option_keys)}

      {key, value} ->
        {:error, {:invalid_pipeline_option, key, value}}
    end
  end

  defp async_handle(client, source, ref),
    do: %AsyncRequest{client: client, source: source, ref: ref, owner: self()}

  defp async_error_handle(client, reason) do
    ref = AsyncDelivery.new()
    AsyncDelivery.deliver(ref, AsyncRequest, {:error, reason})
    async_handle(client, self(), ref)
  end

  defp async_source(client) when is_pid(client) do
    case ClientSupervisor.coordinator(client) do
      {:ok, coordinator} -> coordinator
      _unavailable -> client
    end
  end

  defp async_source(_invalid), do: self()
end
