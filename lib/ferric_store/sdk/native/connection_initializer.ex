defmodule FerricStore.SDK.Native.ConnectionInitializer do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    ConnectionEncoder,
    ConnectionEventHandler,
    ConnectionOptions
  }

  alias FerricStore.Transport.{ServerFrameAssembler, Socket}

  def run(endpoint, state_module) when is_atom(state_module) do
    case Socket.connect(endpoint) do
      {:ok, transport, socket} ->
        endpoint
        |> build_state(state_module, transport, socket)
        |> activate()

      {:error, reason} ->
        {:stop, reason}
    end
  end

  def activate(state) do
    case Socket.set_active_once(state.transport, state.socket) do
      :ok ->
        {:ok, %{state | encoder: ConnectionEncoder.start(self())}}

      {:error, reason} ->
        close_socket(Socket, state.transport, state.socket)
        {:stop, reason}
    end
  end

  def activate(state, socket_module, encoder_module) do
    case socket_module.set_active_once(state.transport, state.socket) do
      :ok ->
        {:ok, %{state | encoder: encoder_module.start(self())}}

      {:error, reason} ->
        close_socket(socket_module, state.transport, state.socket)
        {:stop, reason}
    end
  end

  defp build_state(endpoint, state_module, transport, socket) do
    policy = ConnectionOptions.effective(endpoint)

    struct!(state_module,
      socket: socket,
      transport: transport,
      endpoint: endpoint,
      event_handler: ConnectionEventHandler.normalize(Map.get(endpoint, :event_handler), self()),
      max_frame_bytes: policy.max_frame_bytes,
      configured_max_request_bytes: policy.max_request_bytes,
      max_request_bytes: policy.max_request_bytes,
      max_response_bytes: policy.max_response_bytes,
      max_response_buffer_bytes: policy.max_response_buffer_bytes,
      configured_max_in_flight: policy.max_in_flight,
      configured_max_in_flight_per_lane: policy.max_in_flight_per_lane,
      max_in_flight: policy.max_in_flight,
      max_in_flight_per_lane: policy.max_in_flight_per_lane,
      server_frame_assembler:
        ServerFrameAssembler.new(
          max_streams: policy.max_server_chunk_streams,
          max_buffer_bytes: policy.max_server_chunk_bytes,
          max_frame_bytes: policy.max_response_bytes,
          timeout: policy.server_chunk_timeout
        ),
      drain: %{active: false, timeout: policy.drain_timeout, timer: nil, token: nil},
      heartbeat_interval: policy.heartbeat_interval,
      heartbeat_timeout: policy.heartbeat_timeout
    )
  end

  defp close_socket(socket_module, transport, socket) do
    socket_module.close(transport, socket)
  rescue
    _error -> :ok
  catch
    _kind, _reason -> :ok
  end
end
