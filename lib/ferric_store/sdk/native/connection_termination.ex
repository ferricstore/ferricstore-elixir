defmodule FerricStore.SDK.Native.ConnectionTermination do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    ConnectionEncoder,
    ConnectionEventHandler,
    ConnectionPendingLifecycle,
    ConnectionServerFrameDecoder,
    ConnectionTimers
  }

  alias FerricStore.Transport.{FrameStream, ServerFrameAssembler, Socket}

  @zero_capacity %{max_in_flight: 0, max_in_flight_per_lane: 0}

  @spec handle({:stop, term(), map()} | term()) ::
          {:stop, term(), map()} | {:noreply, map()} | term()
  def handle({:stop, reason, state}) when is_map(state) do
    state =
      ConnectionPendingLifecycle.fail_all(
        state,
        {:transport_failed, {:connection_down, reason}}
      )

    if ConnectionPendingLifecycle.awaiting_delivery?(state),
      do: {:noreply, terminalize(state)},
      else: {:stop, reason, state}
  end

  def handle(result), do: result

  defp terminalize(%{drain: %{terminal: true}} = state), do: state

  defp terminalize(state) do
    ConnectionTimers.cancel(state.heartbeat_timer)
    ConnectionTimers.cancel(state.drain.timer)
    ConnectionServerFrameDecoder.stop(state.decode)
    ServerFrameAssembler.cancel_timers(state.server_frame_assembler)
    ConnectionEncoder.stop(state.encoder)
    close_socket(state)
    ConnectionEventHandler.capacity_changed(state.event_handler, self(), @zero_capacity)

    token = make_ref()
    timer = Process.send_after(self(), {:drain_timeout, token}, state.drain.timeout)

    drain =
      Map.merge(state.drain, %{
        active: true,
        terminal: true,
        timer: timer,
        token: token
      })

    %{
      state
      | socket: nil,
        encoder: nil,
        buffer: FrameStream.new(),
        decode: nil,
        max_in_flight: 0,
        max_in_flight_per_lane: 0,
        drain: drain,
        heartbeat_timer: nil,
        heartbeat_token: nil
    }
  end

  defp close_socket(%{socket: nil}), do: :ok

  defp close_socket(%{transport: transport, socket: socket}) do
    Socket.close(transport, socket)
  rescue
    _error -> :ok
  catch
    _kind, _reason -> :ok
  end
end
