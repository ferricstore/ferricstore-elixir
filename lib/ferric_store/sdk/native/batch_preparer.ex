defmodule FerricStore.SDK.Native.BatchPreparer do
  @moduledoc false

  use GenServer

  alias FerricStore.{FailureFormatter, RequestContext}

  alias FerricStore.SDK.Native.{
    BatchGroupPolicy,
    BatchGroupPreparer,
    BatchRestoredPreparation,
    BatchRouter,
    KVBatchRestorer,
    Topology
  }

  @enforce_keys [
    :owner,
    :token,
    :batch_id,
    :topology,
    :items,
    :key_fun,
    :payload_builder,
    :group_preparer,
    :mode,
    :context
  ]
  defstruct @enforce_keys ++ [item_restorer: nil]

  @type t :: %__MODULE__{
          owner: pid(),
          token: reference(),
          batch_id: reference(),
          topology: Topology.t(),
          items: list() | map(),
          key_fun: (term() -> term()),
          payload_builder: (list() -> map()),
          group_preparer: function() | nil,
          item_restorer: KVBatchRestorer.t() | nil,
          mode: :standard | :compact | :restore_compact,
          context: RequestContext.t()
        }

  def start(supervisor, %__MODULE__{owner: owner, token: token} = operation)
      when is_pid(supervisor) and is_pid(owner) and is_reference(token) do
    DynamicSupervisor.start_child(supervisor, {__MODULE__, operation})
  end

  def child_spec(args) do
    %{
      id: {__MODULE__, make_ref()},
      start: {__MODULE__, :start_link, [args]},
      restart: :temporary
    }
  end

  def start_link(args), do: GenServer.start_link(__MODULE__, args)

  @impl true
  def init(args), do: {:ok, args, {:continue, :prepare}}

  @impl true
  def handle_continue(:prepare, %__MODULE__{} = operation) do
    result = prepare_operation(operation)

    send(
      operation.owner,
      {:batch_prepared, self(), operation.token, operation.batch_id, result}
    )

    {:stop, :normal, nil}
  end

  defp prepare_operation(%__MODULE__{mode: :standard} = operation) do
    prepare(
      operation.topology,
      operation.items,
      operation.key_fun,
      operation.payload_builder,
      operation.context
    )
  end

  defp prepare_operation(%__MODULE__{mode: :compact, group_preparer: group_preparer} = operation)
       when is_function(group_preparer, 1) do
    prepare_compact(
      operation.topology,
      operation.items,
      operation.key_fun,
      operation.payload_builder,
      group_preparer,
      operation.context
    )
  end

  defp prepare_operation(%__MODULE__{mode: :restore_compact} = operation),
    do: BatchRestoredPreparation.run(operation, &prepare_compact/6)

  defp prepare_operation(_operation), do: {:error, :invalid_batch_preparation}

  @spec prepare(
          Topology.t(),
          list() | map(),
          (term() -> term()),
          (list() -> term()),
          RequestContext.t()
        ) ::
          {:ok, [map()]} | {:error, term()}
  def prepare(topology, items, key_fun, payload_builder, %RequestContext{} = context) do
    item_router = &route_with_key_fun(key_fun, &1)

    with {:ok, groups} <- BatchRouter.route(topology, items, item_router, context),
         :ok <- BatchGroupPolicy.validate(groups, RequestContext.options(context)) do
      BatchGroupPreparer.prepare(
        groups,
        payload_builder,
        &{:ok, &1},
        :retain_items,
        context
      )
    end
  end

  @doc false
  @spec prepare_compact(
          Topology.t(),
          list() | map(),
          BatchRouter.item_router(),
          (list() -> term()),
          (map() -> {:ok, map()} | {:error, term()}),
          RequestContext.t()
        ) :: {:ok, [map()]} | {:error, term()}
  def prepare_compact(
        topology,
        items,
        item_router,
        payload_builder,
        group_preparer,
        %RequestContext{} = context
      )
      when is_function(item_router, 1) and is_function(payload_builder, 1) and
             is_function(group_preparer, 1) do
    with {:ok, groups} <- BatchRouter.route(topology, items, item_router, context),
         :ok <- BatchGroupPolicy.validate(groups, RequestContext.options(context)) do
      BatchGroupPreparer.prepare(
        groups,
        payload_builder,
        group_preparer,
        :discard_items,
        context
      )
    end
  end

  defp route_with_key_fun(key_fun, item) do
    case apply_route_key_fun(key_fun, item) do
      key when is_binary(key) -> {:ok, key, item}
      {:group_by, key, grouping} when is_binary(key) -> {:ok, key, item, grouping}
      {:error, _reason} = error -> error
      other -> {:error, {:invalid_route_key, other}}
    end
  end

  defp apply_route_key_fun(key_fun, item) do
    key_fun.(item)
  rescue
    error ->
      {:error, {:route_key_failed, FailureFormatter.exception_message(error, "route key failed")}}
  catch
    kind, reason -> {:error, {:route_key_failed, {kind, reason}}}
  end
end
