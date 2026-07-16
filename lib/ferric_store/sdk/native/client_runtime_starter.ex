defmodule FerricStore.SDK.Native.ClientRuntimeStarter do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    AdmissionGate,
    ClientEndpoint,
    ClientOptions,
    ClientOwnerGuard,
    Coordinator,
    EventFanout
  }

  @default_max_event_queue 1_024
  @default_max_pending_requests 1_024

  @spec start(pid(), pid(), :ets.tid(), keyword()) :: {:ok, pid()} | {:error, term()}
  def start(supervisor, owner, endpoint, opts) do
    max_event_queue =
      ClientOptions.positive_integer(opts, :max_event_queue, @default_max_event_queue)

    pending = :atomics.new(1, signed: false)
    delivery_token = make_ref()

    submission_admission =
      opts
      |> ClientOptions.positive_integer(
        :max_pending_requests,
        @default_max_pending_requests
      )
      |> AdmissionGate.new()

    :ok = ClientEndpoint.put_submission_admission(endpoint, submission_admission)

    with {:ok, _guard} <-
           start_child(
             supervisor,
             child(:owner_guard, {ClientOwnerGuard, :start_link, [owner]}, :worker)
           ),
         {:ok, connection_supervisor} <-
           start_child(supervisor, dynamic_supervisor_child(:connections)),
         {:ok, operation_supervisor} <-
           start_child(supervisor, dynamic_supervisor_child(:operations)),
         {:ok, fanout_pid} <-
           start_child(
             supervisor,
             child(
               :event_fanout,
               {EventFanout, :start_supervised, [{supervisor, pending, delivery_token}]},
               :worker
             )
           ) do
      :ok = ClientEndpoint.put_event_source(endpoint, fanout_pid)
      event_fanout = EventFanout.handle(fanout_pid, pending, max_event_queue, delivery_token)

      runtime_opts =
        Keyword.merge(opts,
          submission_admission: submission_admission,
          runtime_supervisor: supervisor,
          connection_supervisor: connection_supervisor,
          operation_supervisor: operation_supervisor,
          event_fanout: event_fanout
        )

      start_coordinator(supervisor, endpoint, runtime_opts)
    else
      {:error, reason} -> stop_with_error(supervisor, reason)
    end
  end

  defp start_coordinator(supervisor, endpoint, runtime_opts) do
    child_spec =
      child(:coordinator, {GenServer, :start_link, [Coordinator, runtime_opts]}, :worker)

    case start_child(supervisor, child_spec) do
      {:ok, coordinator} -> finish_start(supervisor, endpoint, coordinator)
      {:error, reason} -> stop_with_error(supervisor, unwrap_coordinator_error(reason))
    end
  end

  defp finish_start(supervisor, endpoint, coordinator) do
    case ClientEndpoint.hand_off(endpoint, coordinator) do
      :ok -> {:ok, supervisor}
      {:error, reason} -> stop_with_error(supervisor, {:endpoint_initialization_failed, reason})
    end
  end

  defp start_child(supervisor, child_spec) do
    case Supervisor.start_child(supervisor, child_spec) do
      {:ok, pid, _info} -> {:ok, pid}
      result -> result
    end
  end

  defp unwrap_coordinator_error(
         {reason,
          {:child, :undefined, :coordinator, _start, :temporary, true, _shutdown, :worker, _}}
       ),
       do: reason

  defp unwrap_coordinator_error(reason), do: reason

  defp dynamic_supervisor_child(id) do
    child(
      id,
      {DynamicSupervisor, :start_link, [[strategy: :one_for_one]]},
      :supervisor,
      :infinity
    )
  end

  defp child(id, start, type, shutdown \\ 5_000) do
    %{
      id: id,
      start: start,
      restart: :temporary,
      significant: true,
      shutdown: shutdown,
      type: type
    }
  end

  defp stop_with_error(supervisor, reason) do
    stop(supervisor)
    {:error, reason}
  end

  defp stop(supervisor) do
    if Process.alive?(supervisor), do: Supervisor.stop(supervisor, :normal), else: :ok
  catch
    :exit, _reason -> :ok
  end
end
