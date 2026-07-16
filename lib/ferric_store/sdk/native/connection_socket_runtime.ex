defmodule FerricStore.SDK.Native.ConnectionSocketRuntime do
  @moduledoc false

  alias FerricStore.SDK.Native.{ConnectionFrameProcessor, ConnectionRequest, ConnectionTimers}
  alias FerricStore.Transport.{FrameStream, Socket}

  @max_frames_per_tick 64

  def data(data, state) do
    state = ConnectionTimers.postpone_active_heartbeat(state)
    state = %{state | buffer: FrameStream.append(state.buffer, data)}
    process_available_frames(state, 0)
  end

  def continue(state), do: process_available_frames(state, 0)

  def down(reason, state),
    do: {:stop, reason, ConnectionRequest.fail_pending(state, {:transport_failed, reason})}

  defp process_available_frames(state, count) when count >= @max_frames_per_tick do
    send(self(), :continue_frames)
    {:noreply, state}
  end

  defp process_available_frames(state, count) do
    case FrameStream.next(state.buffer, state.max_frame_bytes) do
      :incomplete ->
        reactivate_socket(state)

      {:ok, header, body, buffer} ->
        state = %{state | buffer: buffer}
        process_frame(header, body, state, count)

      {:error, reason} ->
        state = %{state | buffer: FrameStream.new()}
        {:stop, reason, ConnectionRequest.fail_pending(state, reason)}
    end
  end

  defp process_frame(header, body, state, count) do
    case ConnectionFrameProcessor.process(header, body, state) do
      {:ok, next_state} -> continue_after_frame(next_state, count)
      {:stop, reason, next_state} -> stop_for_frame_error(reason, next_state)
    end
  end

  defp continue_after_frame(%{decode: nil} = state, count),
    do: process_available_frames(state, count + 1)

  defp continue_after_frame(state, _count), do: {:noreply, state}

  defp stop_for_frame_error(reason, state),
    do: {:stop, reason, ConnectionRequest.fail_pending(state, reason)}

  defp reactivate_socket(%{transport: transport, socket: socket} = state) do
    case Socket.set_active_once(transport, socket) do
      :ok -> {:noreply, state}
      {:error, reason} -> down(reason, state)
    end
  end
end
