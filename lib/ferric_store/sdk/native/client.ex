defmodule FerricStore.SDK.Native.Client do
  @moduledoc """
  FerricStore native protocol client with topology-aware shard routing.

  This public facade performs validation before sending work to the internal
  coordinator. Socket ownership remains in native connection processes.
  """

  alias FerricStore.SDK.Native.{ClientBatchRequests, ClientRequests, PipelineRequests, Topology}

  @default_timeout 5_000

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) when is_list(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      shutdown: :infinity,
      type: :supervisor
    }
  end

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: ClientRequests.start_link(opts)

  @spec close(pid(), timeout()) ::
          :ok | {:error, {:invalid_client, atom()} | {:close_failed, term()}}
  def close(client, timeout \\ @default_timeout), do: ClientRequests.close(client, timeout)

  @spec cancel_async(term(), term(), term(), term()) ::
          :ok | {:error, {:cancel_failed, term()}}
  def cancel_async(client, owner, ref, timeout \\ @default_timeout)

  def cancel_async(client, owner, ref, timeout)
      when is_pid(client) and is_pid(owner) and is_reference(ref),
      do: ClientRequests.cancel_async(client, owner, ref, timeout)

  def cancel_async(client, _owner, _ref, _timeout) when not is_pid(client),
    do: invalid_cancellation(:invalid_client)

  def cancel_async(_client, owner, _ref, _timeout) when not is_pid(owner),
    do: invalid_cancellation(:invalid_owner)

  def cancel_async(_client, _owner, _ref, _timeout),
    do: invalid_cancellation(:invalid_reference)

  @spec from_url(binary(), keyword()) :: GenServer.on_start()
  def from_url(url, opts \\ []), do: ClientRequests.from_url(url, opts)

  @spec subscribe_events(pid(), [term()], keyword()) :: {:ok, term()} | {:error, term()}
  def subscribe_events(client, events \\ [], opts \\ []),
    do: ClientRequests.event_subscription(client, :subscribe, events, opts)

  @spec unsubscribe_events(pid(), [term()], keyword()) :: {:ok, term()} | {:error, term()}
  def unsubscribe_events(client, events \\ [], opts \\ []),
    do: ClientRequests.event_subscription(client, :unsubscribe, events, opts)

  @spec await_event(pid(), timeout()) ::
          {:ok, map()}
          | {:error, :client_closed | {:client_unavailable, :invalid_client}}
          | {:error, {:invalid_timeout, term()}}
          | nil
  def await_event(client, timeout \\ @default_timeout),
    do: ClientRequests.await_event(client, timeout)

  @spec ping(pid(), term(), keyword()) :: {:ok, term()} | {:error, term()}
  def ping(client, message \\ "PONG", opts \\ []),
    do: ClientRequests.ping(client, message, opts)

  @spec command_exec(pid(), binary(), list(), keyword()) :: {:ok, term()} | {:error, term()}
  def command_exec(client, command, args \\ [], opts \\ []),
    do: ClientRequests.command_exec(client, command, args, opts)

  @spec pipeline(pid(), list(), keyword(), keyword()) :: {:ok, term()} | {:error, term()}
  def pipeline(client, commands, pipeline_options, opts),
    do: PipelineRequests.request(client, commands, pipeline_options, opts)

  @spec async_pipeline(pid(), list(), keyword(), keyword()) :: reference()
  def async_pipeline(client, commands, pipeline_options, opts),
    do: PipelineRequests.async_request(client, commands, pipeline_options, opts)

  @spec request(pid(), non_neg_integer() | atom() | binary(), term(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def request(client, opcode, payload \\ %{}, opts \\ []),
    do: ClientRequests.request(client, opcode, payload, opts)

  @doc false
  @spec request_trusted_batch(
          pid(),
          non_neg_integer() | atom() | binary(),
          term(),
          non_neg_integer(),
          keyword()
        ) ::
          {:ok, term()} | {:error, term()}
  def request_trusted_batch(client, opcode, payload, item_count, opts),
    do: ClientBatchRequests.request_trusted(client, opcode, payload, item_count, opts)

  @spec async_request(pid(), non_neg_integer() | atom() | binary(), term(), keyword()) ::
          reference()
  def async_request(client, opcode, payload \\ %{}, opts \\ []),
    do: ClientRequests.async_request(client, opcode, payload, opts)

  @spec request_by_key(pid(), non_neg_integer() | atom() | binary(), binary(), map(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def request_by_key(client, opcode, key, payload, opts \\ []),
    do: ClientRequests.request_by_key(client, opcode, key, payload, opts)

  @spec async_request_by_key(
          pid(),
          non_neg_integer() | atom() | binary(),
          binary(),
          map(),
          keyword()
        ) :: reference()
  def async_request_by_key(client, opcode, key, payload, opts \\ []),
    do: ClientRequests.async_request_by_key(client, opcode, key, payload, opts)

  @spec request_by_keys(
          pid(),
          non_neg_integer() | atom() | binary(),
          [binary()],
          ([binary()] -> map()),
          keyword()
        ) :: {:ok, [map()]} | {:error, term()}
  def request_by_keys(client, opcode, keys, payload_builder, opts \\ []) do
    ClientRequests.request_by_items(
      client,
      opcode,
      keys,
      & &1,
      payload_builder,
      opts
    )
  end

  @spec request_by_items(
          pid(),
          non_neg_integer() | atom() | binary(),
          list(),
          (term() -> binary()),
          (list() -> map()),
          keyword()
        ) :: {:ok, [map()]} | {:error, term()}
  def request_by_items(client, opcode, items, key_fun, payload_builder, opts \\ []) do
    ClientRequests.request_by_items(
      client,
      opcode,
      items,
      key_fun,
      payload_builder,
      opts
    )
  end

  @spec topology(pid()) :: Topology.t() | {:error, term()}
  def topology(client), do: ClientRequests.topology(client)

  @spec route(pid(), term()) :: {:ok, map()} | {:error, term()}
  def route(client, key), do: ClientRequests.route(client, key)

  @spec refresh_topology(pid(), timeout()) :: :ok | {:error, term()}
  def refresh_topology(client, timeout \\ @default_timeout),
    do: ClientRequests.refresh_topology(client, timeout)

  defp invalid_cancellation(reason),
    do: {:error, {:cancel_failed, {:invalid_async_request, reason}}}
end
