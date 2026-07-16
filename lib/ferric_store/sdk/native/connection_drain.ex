defmodule FerricStore.SDK.Native.ConnectionDrain do
  @moduledoc false

  alias FerricStore.SDK.Native.ConnectionTimers

  @spec begin(map()) :: map()
  def begin(%{drain: %{active: true}} = state), do: state

  def begin(state) do
    token = make_ref()
    timer = Process.send_after(self(), {:drain_timeout, token}, state.drain.timeout)

    state
    |> ConnectionTimers.cancel_heartbeat()
    |> then(&%{&1 | drain: %{&1.drain | active: true, token: token, timer: timer}})
    |> maybe_stop()
  end

  @spec maybe_stop(map()) :: map()
  def maybe_stop(%{drain: %{active: true} = drain, pending: pending} = state)
      when map_size(pending) == 0 do
    ConnectionTimers.cancel(drain.timer)
    send(self(), :stop_when_drained)
    %{state | drain: %{drain | timer: nil, token: nil}}
  end

  def maybe_stop(state), do: state
end
