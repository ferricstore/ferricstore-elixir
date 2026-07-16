defmodule FerricStore.SDK.Native.Admission do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    BatchScheduler,
    EventCoordinator,
    PreparationReservations,
    RequestRegistry,
    TopologyManager
  }

  defstruct batch_groups: 0

  @type t :: %__MODULE__{batch_groups: non_neg_integer()}

  @spec full?(map()) :: boolean()
  def full?(
        %{
          admission: %__MODULE__{} = admission,
          request_registry: request_registry,
          batch_scheduler: batch_scheduler,
          event_coordinator: event_coordinator,
          topology_manager: topology_manager,
          limits: %{pending_requests: limit}
        } = state
      ) do
    full?(
      admission,
      RequestRegistry.size(request_registry),
      BatchScheduler.size(batch_scheduler),
      EventCoordinator.queue_size(event_coordinator),
      TopologyManager.refresh_call_count(topology_manager),
      PreparationReservations.size(state.preparation_reservations),
      limit
    )
  end

  @spec full?(
          t(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer(),
          pos_integer()
        ) :: boolean()
  def full?(
        %__MODULE__{batch_groups: batch_groups},
        pending,
        batches,
        events,
        refreshes,
        preparations,
        limit
      ) do
    logical_pending = max(pending - batch_groups, 0)
    logical_pending + batches + events + refreshes + preparations >= limit
  end

  @spec wire_slots(pos_integer(), non_neg_integer()) :: non_neg_integer()
  def wire_slots(limit, pending), do: max(limit - pending, 0)

  @spec adjust_batch_groups(t(), integer()) :: t()
  def adjust_batch_groups(%__MODULE__{} = admission, delta) when is_integer(delta) do
    %{admission | batch_groups: max(admission.batch_groups + delta, 0)}
  end
end
