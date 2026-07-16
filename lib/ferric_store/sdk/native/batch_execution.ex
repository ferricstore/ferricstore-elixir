defmodule FerricStore.SDK.Native.BatchExecution do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    BatchCompletion,
    BatchPreflightReservations,
    BatchRequestCancellation,
    BatchWireExecution
  }

  alias FerricStore.SDK.Native.Coordinator.State

  @type action :: {:continue, State.t()} | {:finish, State.t()} | {:timeout, State.t()}

  @spec advance(State.t(), reference()) :: action()
  defdelegate advance(state, batch_id), to: BatchWireExecution

  @spec handle_result(State.t(), map(), term()) :: action()
  defdelegate handle_result(state, request, result), to: BatchWireExecution

  @spec record_preflight(map(), map(), map(), {:ok, pid() | nil} | {:error, term()}) :: map()
  defdelegate record_preflight(batch, group, connecting_groups, result),
    to: BatchPreflightReservations,
    as: :record

  @spec release_preflight(State.t(), map() | [map()]) :: State.t() | {State.t(), [map()]}
  defdelegate release_preflight(state, batch_or_groups),
    to: BatchPreflightReservations,
    as: :release

  @spec cancel_requests(State.t(), map()) :: State.t()
  defdelegate cancel_requests(state, batch), to: BatchRequestCancellation, as: :cancel

  @spec take_completion(State.t(), reference()) ::
          {{:ok, [map()]} | {:retry, term()} | {:error, term()}, State.t(), map()}
  defdelegate take_completion(state, batch_id), to: BatchCompletion, as: :take
end
