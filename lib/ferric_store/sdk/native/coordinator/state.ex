defmodule FerricStore.SDK.Native.Coordinator.State do
  @moduledoc false

  alias FerricStore.RequestLimits

  alias FerricStore.SDK.Native.{
    Admission,
    BatchScheduler,
    ClientOptions,
    ClientSeedOptions,
    ConnectionPool,
    EndpointPolicy,
    EndpointTrust,
    EventCoordinator,
    LifecycleRegistry,
    PreparationReservations,
    RequestRegistry,
    TopologyManager
  }

  alias FerricStore.SDK.Native.Coordinator.{PendingRequests, StateEvents, StateLifecycle}

  @default_timeout 5_000
  @default_max_pending_requests 1_024
  @default_max_connecting 32
  @default_max_connections 64
  @default_connections_per_endpoint 2
  @default_max_batch_items RequestLimits.max_batch_items()
  @default_max_refresh_candidates ClientSeedOptions.default_candidate_limit()
  @default_max_event_subscribers 1_024

  defstruct [
    :username,
    :password,
    :tls,
    :server_name,
    :client_name,
    :endpoint_validator,
    :endpoint_policy,
    :endpoint_trust,
    :endpoint_options,
    :warmup,
    :runtime_supervisor,
    :connection_supervisor,
    :operation_supervisor,
    :event_fanout,
    :submission_admission,
    limits: %{
      pending_requests: @default_max_pending_requests,
      batch_items: @default_max_batch_items,
      event_subscribers: @default_max_event_subscribers
    },
    admission: %Admission{},
    seeds: [],
    connection_pool: %ConnectionPool{
      max_connections: @default_max_connections,
      max_connecting: @default_max_connecting,
      connections_per_endpoint: @default_connections_per_endpoint
    },
    request_registry: %RequestRegistry{},
    lifecycle_registry: %LifecycleRegistry{},
    preparation_reservations: %PreparationReservations{},
    batch_scheduler: %BatchScheduler{},
    topology_manager: %TopologyManager{},
    event_coordinator: %EventCoordinator{},
    topology_refresh_timeout: @default_timeout,
    max_refresh_candidates: @default_max_refresh_candidates
  ]

  @type t :: %__MODULE__{}

  @spec new(keyword(), [term()], map()) :: t()
  def new(opts, seeds, endpoint_options) do
    defaults = %__MODULE__{}

    struct!(__MODULE__,
      seeds: seeds,
      username: Keyword.get(opts, :username),
      password: Keyword.get(opts, :password),
      tls: Keyword.get(opts, :tls, false),
      server_name: Keyword.get(opts, :server_name),
      endpoint_validator: Keyword.get(opts, :endpoint_validator),
      endpoint_policy:
        opts
        |> Keyword.get(:endpoint_policy, :seed_hosts)
        |> EndpointPolicy.compile(),
      endpoint_trust: EndpointTrust.new(seeds, opts),
      endpoint_options: endpoint_options,
      warmup: %{enabled: Keyword.get(opts, :warm_connections, false), queue: :queue.new()},
      runtime_supervisor: Keyword.fetch!(opts, :runtime_supervisor),
      connection_supervisor: Keyword.fetch!(opts, :connection_supervisor),
      operation_supervisor: Keyword.fetch!(opts, :operation_supervisor),
      event_fanout: Keyword.fetch!(opts, :event_fanout),
      submission_admission: Keyword.fetch!(opts, :submission_admission),
      limits: %{
        pending_requests:
          ClientOptions.positive_integer(
            opts,
            :max_pending_requests,
            defaults.limits.pending_requests
          ),
        batch_items:
          ClientOptions.positive_integer(opts, :max_batch_items, defaults.limits.batch_items),
        event_subscribers:
          ClientOptions.positive_integer(
            opts,
            :max_event_subscribers,
            defaults.limits.event_subscribers
          )
      },
      connection_pool:
        ConnectionPool.new(
          max_connecting:
            ClientOptions.positive_integer(
              opts,
              :max_connecting,
              defaults.connection_pool.max_connecting
            ),
          max_connections:
            ClientOptions.positive_integer(
              opts,
              :max_connections,
              defaults.connection_pool.max_connections
            ),
          connections_per_endpoint:
            ClientOptions.positive_integer(
              opts,
              :connections_per_endpoint,
              defaults.connection_pool.connections_per_endpoint
            )
        ),
      topology_refresh_timeout:
        ClientOptions.positive_integer(
          opts,
          :topology_refresh_timeout,
          defaults.topology_refresh_timeout
        ),
      max_refresh_candidates:
        ClientOptions.positive_integer(
          opts,
          :max_refresh_candidates,
          defaults.max_refresh_candidates
        ),
      client_name: Keyword.get(opts, :client_name, "ferricstore-elixir-sdk")
    )
  end

  def put_pending_request(state, tag, request), do: PendingRequests.put(state, tag, request)
  def pop_pending_request(state, tag), do: PendingRequests.pop(state, tag)

  def put_lifecycle_monitor(state, monitor, owner),
    do: StateLifecycle.put_monitor(state, monitor, owner)

  def delete_lifecycle_monitor(state, monitor, owner),
    do: StateLifecycle.delete_monitor(state, monitor, owner)

  def adjust_batch_groups(state, delta), do: StateLifecycle.adjust_batch_groups(state, delta)
  def adjust_refresh_calls(state, delta), do: StateLifecycle.adjust_refresh_calls(state, delta)

  def event_subscriptions(state), do: StateEvents.subscriptions(state)
  def event_restore(state), do: StateEvents.restore(state)
  def put_event_restore(state, restore), do: StateEvents.put_restore(state, restore)
  def event_operation(state), do: StateEvents.operation(state)
  def put_event_operation(state, operation), do: StateEvents.put_operation(state, operation)

  def clear_event_connection(state, connection),
    do: StateEvents.clear_connection(state, connection)

  def put_event_connection(state, connection), do: StateEvents.put_connection(state, connection)
  def event_connection(state), do: StateEvents.connection(state)
  def live_event_connection?(state), do: StateEvents.live_connection?(state)
  def event_subscriptions_empty?(state), do: StateEvents.subscriptions_empty?(state)

  def reserve_event_subscriber(state, subscriber, limit),
    do: StateEvents.reserve_subscriber(state, subscriber, limit)

  def release_event_subscriber(state, subscriber),
    do: StateEvents.release_subscriber(state, subscriber)

  def subscribe_events(state, subscriber, events, connection),
    do: StateEvents.subscribe(state, subscriber, events, connection)

  def unsubscribe_events(state, subscriber, events),
    do: StateEvents.unsubscribe(state, subscriber, events)
end
