defmodule FerricStore.SDK.Native.ConnectionShutdown do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    ConnectionEncoder,
    ConnectionEventHandler,
    ConnectionResponseDecoder,
    ConnectionServerFrameDecoder,
    ConnectionTimers
  }

  alias FerricStore.Transport.{ServerFrameAssembler, Socket}

  @spec run(map()) :: :ok
  def run(state) do
    ConnectionTimers.cancel_pending(state.pending)
    ConnectionResponseDecoder.stop_pending(state.pending)
    ConnectionServerFrameDecoder.stop(state.decode)
    ServerFrameAssembler.cancel_timers(state.server_frame_assembler)
    ConnectionTimers.cancel(state.heartbeat_timer)
    ConnectionTimers.cancel(state.drain.timer)
    ConnectionEncoder.stop(state.encoder)
    ConnectionEventHandler.stop(state.event_handler)
    close_socket(state)
  end

  defp close_socket(%{socket: nil}), do: :ok

  defp close_socket(%{transport: transport, socket: socket}) do
    Socket.close(transport, socket)
  rescue
    _error -> :ok
  catch
    _, _ -> :ok
  end
end
