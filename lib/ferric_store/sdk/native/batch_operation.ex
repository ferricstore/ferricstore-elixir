defmodule FerricStore.SDK.Native.BatchOperation do
  @moduledoc false

  alias FerricStore.{RequestContext, RequestLimits}
  alias FerricStore.SDK.Native.ClientOptions

  @default_max_concurrency 32

  @enforce_keys [:id, :from, :opcode, :items, :item_count, :key_fun, :payload_builder, :opts]
  defstruct [
    :id,
    :from,
    :opcode,
    :operation,
    :items,
    :item_count,
    :key_fun,
    :payload_builder,
    :opts,
    group_preparer: nil,
    item_restorer: nil,
    preparation_mode: :standard,
    attempt: 0,
    original_reason: nil,
    max_concurrency: @default_max_concurrency,
    phase: :routing,
    preparer: nil,
    timer: nil,
    caller_monitor: nil,
    connections_remaining: 0,
    connections_inflight: 0,
    connecting_groups: %{},
    connection_queue: [],
    ready_groups: [],
    queued: [],
    inflight: 0,
    request_tags: MapSet.new(),
    successes: [],
    failures: []
  ]

  @type t :: %__MODULE__{
          id: reference(),
          from: GenServer.from(),
          opcode: non_neg_integer(),
          operation: :del | :mget | :mset | nil,
          items: list() | map() | nil,
          item_count: non_neg_integer(),
          key_fun: function(),
          payload_builder: function(),
          opts: RequestContext.t(),
          group_preparer: function() | nil,
          item_restorer: term(),
          preparation_mode: :standard | :compact | :restore_compact,
          attempt: non_neg_integer(),
          original_reason: term(),
          max_concurrency: pos_integer(),
          phase: atom(),
          preparer: map() | nil,
          timer: reference() | nil,
          caller_monitor: reference() | nil,
          connections_remaining: non_neg_integer(),
          connections_inflight: non_neg_integer(),
          connecting_groups: %{optional(non_neg_integer()) => map()},
          connection_queue: list(),
          ready_groups: list(),
          queued: list(),
          inflight: non_neg_integer(),
          request_tags: MapSet.t(),
          successes: list(),
          failures: list()
        }

  @spec new(
          GenServer.from(),
          non_neg_integer(),
          list() | map(),
          non_neg_integer(),
          function(),
          function(),
          RequestContext.t()
        ) :: t()
  def new(
        from,
        opcode,
        items,
        item_count,
        key_fun,
        payload_builder,
        %RequestContext{} = opts
      ) do
    build(from, opcode, nil, items, item_count, key_fun, payload_builder, opts)
  end

  @spec new_prepared(
          GenServer.from(),
          non_neg_integer(),
          :del | :mget | :mset,
          non_neg_integer(),
          function(),
          function(),
          RequestContext.t()
        ) :: t()
  def new_prepared(
        from,
        opcode,
        operation,
        item_count,
        key_fun,
        payload_builder,
        %RequestContext{} = opts
      )
      when operation in [:del, :mget, :mset] do
    build(from, opcode, operation, nil, item_count, key_fun, payload_builder, opts)
  end

  defp build(from, opcode, operation, items, item_count, key_fun, payload_builder, opts) do
    %__MODULE__{
      id: make_ref(),
      from: from,
      opcode: opcode,
      operation: operation,
      items: items,
      item_count: item_count,
      key_fun: key_fun,
      payload_builder: payload_builder,
      opts: opts,
      max_concurrency:
        opts
        |> RequestContext.options()
        |> ClientOptions.positive_integer(:max_group_concurrency, @default_max_concurrency)
        |> min(RequestLimits.max_group_concurrency())
    }
  end
end
