defmodule FerricStore.SDK.Native.ConnectionTimers do
  @moduledoc false

  alias FerricStore.Timeout

  @default_timeout 5_000

  @spec request_timer(non_neg_integer(), reference(), timeout()) :: reference() | nil
  def request_timer(_request_id, _token, :infinity), do: nil

  def request_timer(request_id, token, timeout) when is_integer(timeout) and timeout >= 0,
    do: Process.send_after(self(), {:request_timeout, request_id, token}, timeout)

  def request_timer(request_id, token, _timeout),
    do: Process.send_after(self(), {:request_timeout, request_id, token}, @default_timeout)

  @spec request_deadline(timeout()) :: :infinity | integer()
  def request_deadline(:infinity), do: :infinity

  def request_deadline(timeout) when is_integer(timeout) and timeout >= 0,
    do: System.monotonic_time(:millisecond) + timeout

  def request_deadline(_timeout),
    do: System.monotonic_time(:millisecond) + @default_timeout

  @spec remaining(:infinity | integer(), timeout()) :: timeout()
  def remaining(:infinity, _timeout), do: :infinity

  def remaining(deadline, _timeout) when is_integer(deadline),
    do: max(deadline - System.monotonic_time(:millisecond), 0)

  def remaining(_deadline, timeout), do: timeout

  @spec expired?(integer() | :infinity) :: boolean()
  def expired?(:infinity), do: false

  def expired?(deadline) when is_integer(deadline),
    do: System.monotonic_time(:millisecond) >= deadline

  @spec cancel_pending(map()) :: :ok
  def cancel_pending(pending) do
    Enum.each(pending, fn {_request_id, request} -> cancel(request.timer) end)
  end

  @spec cancel(reference() | nil) :: :ok
  def cancel(nil), do: :ok

  def cancel(timer) do
    Process.cancel_timer(timer, async: true, info: false)
    :ok
  end

  @spec schedule_heartbeat(map()) :: map()
  def schedule_heartbeat(%{drain: %{active: true}} = state), do: state
  def schedule_heartbeat(%{heartbeat_interval: :infinity} = state), do: state

  def schedule_heartbeat(%{heartbeat_interval: interval} = state)
      when is_integer(interval) and interval > 0 do
    cancel(state.heartbeat_timer)
    token = make_ref()
    timer = Process.send_after(self(), {:heartbeat, token}, interval)
    %{state | heartbeat_timer: timer, heartbeat_token: token}
  end

  @spec postpone_active_heartbeat(map()) :: map()
  def postpone_active_heartbeat(%{heartbeat_timer: timer, heartbeat_token: token} = state)
      when is_reference(timer) and is_reference(token),
      do: schedule_heartbeat(state)

  def postpone_active_heartbeat(state), do: state

  @spec cancel_heartbeat(map()) :: map()
  def cancel_heartbeat(state) do
    cancel(state.heartbeat_timer)
    %{state | heartbeat_timer: nil, heartbeat_token: nil}
  end

  @spec call_timeout(timeout()) :: timeout()
  def call_timeout(:infinity), do: :infinity

  def call_timeout(timeout) when is_integer(timeout) and timeout >= 0,
    do: Timeout.add_margin(timeout, 1_000)

  def call_timeout(_timeout), do: @default_timeout + 1_000
end
