defmodule FerricStore.SDK.Native.ConnectionInfoRuntime do
  @moduledoc false

  alias FerricStore.Protocol.CommandSpec

  alias FerricStore.SDK.Native.{
    ConnectionDiscardedResponse,
    ConnectionDrain,
    ConnectionDrainTimeoutRuntime,
    ConnectionEncoder,
    ConnectionRequest,
    ConnectionResponseDelivery,
    ConnectionResponseRuntime,
    ConnectionServerFrameRuntime,
    ConnectionSocketRuntime,
    ConnectionTimeoutRuntime,
    ConnectionTimers
  }

  alias FerricStore.Transport.ServerFrameAssembler

  @ping_opcode CommandSpec.fetch!(:ping).opcode

  def handle({:tcp, socket, data}, %{transport: :gen_tcp, socket: socket} = state),
    do: ConnectionSocketRuntime.data(data, state)

  def handle({:ssl, socket, data}, %{transport: :ssl, socket: socket} = state),
    do: ConnectionSocketRuntime.data(data, state)

  def handle({:request_timeout, request_id, token}, state),
    do: ConnectionTimeoutRuntime.handle(request_id, token, state)

  def handle(
        {:ferricstore_response_decoded, worker, request_id, decode_token, result},
        state
      ) do
    case ConnectionResponseRuntime.complete_decode(
           state,
           worker,
           request_id,
           decode_token,
           result
         ) do
      {:ok, next_state} -> {:noreply, next_state}
      {:stop, reason, next_state} -> {:stop, reason, next_state}
    end
  end

  def handle({:ferricstore_server_frame_decoded, worker, decode_token, result}, state),
    do: ConnectionServerFrameRuntime.complete(state, worker, decode_token, result)

  def handle({:ferricstore_server_frame_delivered, worker, decode_token, outcome}, state),
    do: ConnectionServerFrameRuntime.delivered(state, worker, decode_token, outcome)

  def handle(
        {:ferricstore_request_encoded, worker, request_id, encode_token,
         {:ready, response_context}},
        state
      ) do
    if ConnectionEncoder.worker?(state.encoder, worker) do
      case ConnectionRequest.encoding_ready(state, request_id, encode_token, response_context) do
        {:authorize, next_state} ->
          :ok = ConnectionEncoder.authorize_send(worker, request_id, encode_token)
          {:noreply, next_state}

        {:discard, next_state} ->
          :ok = ConnectionEncoder.discard(worker, request_id, encode_token)
          {:noreply, ConnectionDrain.maybe_stop(next_state)}
      end
    else
      {:noreply, state}
    end
  end

  def handle(
        {:ferricstore_request_encoded, worker, request_id, encode_token, result},
        state
      ) do
    if ConnectionEncoder.worker?(state.encoder, worker) do
      case ConnectionRequest.complete_encoding(state, request_id, encode_token, result) do
        {:ok, next_state} -> {:noreply, ConnectionDrain.maybe_stop(next_state)}
        {:stop, reason, next_state} -> {:stop, reason, next_state}
      end
    else
      {:noreply, state}
    end
  end

  def handle({:heartbeat, token}, %{heartbeat_token: token, drain: %{active: false}} = state) do
    state = %{state | heartbeat_timer: nil, heartbeat_token: nil}

    case ConnectionRequest.submit(
           state,
           :heartbeat,
           @ping_opcode,
           %{},
           0,
           state.heartbeat_timeout
         ) do
      {:ok, next_state} ->
        {:noreply, next_state}

      {:error, :connection_backpressure, next_state} ->
        {:noreply, ConnectionTimers.schedule_heartbeat(next_state)}

      {:error, reason, next_state} ->
        {:stop, {:heartbeat_failed, reason}, next_state}
    end
  end

  def handle({:heartbeat, _token}, state), do: {:noreply, state}

  def handle({:drain_timeout, token}, state),
    do: ConnectionDrainTimeoutRuntime.handle(token, state)

  def handle({:late_response_timeout, request_id, token}, state),
    do: ConnectionDiscardedResponse.expire(state, request_id, token)

  def handle({:ferricstore_response_delivered, reply_to, tag, delivery_token}, state) do
    next_state =
      ConnectionResponseDelivery.acknowledge(state, reply_to, tag, delivery_token)

    {:noreply, next_state}
  end

  def handle({:server_chunk_timeout, key, token}, state) do
    if ServerFrameAssembler.timeout?(state.server_frame_assembler, key, token) do
      {:stop, :server_chunk_timeout, ConnectionRequest.fail_pending(state, :server_chunk_timeout)}
    else
      {:noreply, state}
    end
  end

  def handle(:continue_frames, %{drain: %{terminal: true}} = state), do: {:noreply, state}
  def handle(:continue_frames, state), do: ConnectionSocketRuntime.continue(state)

  def handle(:stop_when_drained, %{drain: %{active: true}, pending: pending} = state)
      when map_size(pending) == 0,
      do: {:stop, :normal, state}

  def handle(:stop_when_drained, state), do: {:noreply, state}

  def handle({:tcp_closed, socket}, %{transport: :gen_tcp, socket: socket} = state),
    do: ConnectionSocketRuntime.down(:closed, state)

  def handle({:ssl_closed, socket}, %{transport: :ssl, socket: socket} = state),
    do: ConnectionSocketRuntime.down(:closed, state)

  def handle({:tcp_error, socket, reason}, %{transport: :gen_tcp, socket: socket} = state),
    do: ConnectionSocketRuntime.down(reason, state)

  def handle({:ssl_error, socket, reason}, %{transport: :ssl, socket: socket} = state),
    do: ConnectionSocketRuntime.down(reason, state)

  def handle(_message, state), do: {:noreply, state}
end
