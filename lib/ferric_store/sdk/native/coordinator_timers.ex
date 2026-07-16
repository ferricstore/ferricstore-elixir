defmodule FerricStore.SDK.Native.CoordinatorTimers do
  @moduledoc false

  alias FerricStore.RequestContext

  @spec pending_request_timer(reference(), RequestContext.t()) :: reference() | nil
  def pending_request_timer(tag, %RequestContext{} = context) do
    case remaining(context) do
      :infinity -> nil
      timeout -> Process.send_after(self(), {:pending_request_timeout, tag}, timeout)
    end
  end

  @spec event_queue_timer(map()) :: reference() | nil
  def event_queue_timer(%{subscriber_down: true}), do: nil

  def event_queue_timer(event_call) do
    case remaining(event_call.opts) do
      :infinity -> nil
      timeout -> Process.send_after(self(), {:event_queue_timeout, event_call.id}, timeout)
    end
  end

  @spec batch_timer(reference(), RequestContext.t()) :: reference() | nil
  def batch_timer(batch_id, %RequestContext{} = context) do
    case remaining(context) do
      :infinity -> nil
      timeout -> Process.send_after(self(), {:batch_timeout, batch_id}, timeout)
    end
  end

  @spec refresh_waiter_timer(reference(), GenServer.from(), RequestContext.t()) ::
          reference() | nil
  def refresh_waiter_timer(monitor, from, %RequestContext{} = context)
      when is_reference(monitor) do
    case remaining(context) do
      :infinity -> nil
      timeout -> Process.send_after(self(), {:refresh_waiter_timeout, monitor, from}, timeout)
    end
  end

  @spec remaining(RequestContext.t()) :: timeout()
  def remaining(%RequestContext{} = context), do: RequestContext.remaining(context)

  @spec expired?(RequestContext.t()) :: boolean()
  def expired?(%RequestContext{} = context), do: remaining(context) == 0

  @spec connection_timeout(RequestContext.t(), pos_integer()) :: timeout()
  def connection_timeout(%RequestContext{} = context, default_timeout),
    do: RequestContext.connection_timeout(context, default_timeout)

  @spec cancel(reference() | nil) :: :ok
  def cancel(nil), do: :ok

  def cancel(timer) do
    Process.cancel_timer(timer, async: true, info: false)
    :ok
  end

  @spec demonitor(reference() | nil) :: :ok
  def demonitor(nil), do: :ok

  def demonitor(monitor) do
    Process.demonitor(monitor, [:flush])
    :ok
  end

  @spec cancel_preparer(map() | nil) :: :ok
  def cancel_preparer(nil), do: :ok

  def cancel_preparer(%{pid: pid, monitor: monitor}) do
    demonitor(monitor)
    if Process.alive?(pid), do: Process.exit(pid, :kill)
    :ok
  end
end
