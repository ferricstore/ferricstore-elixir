defmodule FerricStore.SDK.Native.ConnectionFrameProcessor do
  @moduledoc false

  alias FerricStore.SDK.Native.{
    Codec,
    ConnectionDiscardedResponse,
    ConnectionResponseRuntime,
    ConnectionServerFrameRuntime
  }

  alias FerricStore.Transport.{
    ResponseAssembler,
    ResponseIdentity,
    ServerFrameAssembler,
    ServerFramePolicy
  }

  @spec process(map(), binary(), map()) :: {:ok, map()} | {:stop, term(), map()}
  def process(
        %{lane_id: lane_id, opcode: opcode, request_id: 0, flags: flags},
        body,
        state
      ) do
    process_server_frame(state, lane_id, opcode, flags, body)
  end

  def process(
        %{lane_id: lane_id, opcode: opcode, request_id: request_id, flags: flags},
        body,
        state
      ) do
    case Map.fetch(state.pending, request_id) do
      :error ->
        {:stop,
         {:unexpected_response, %{lane_id: lane_id, opcode: opcode, request_id: request_id}},
         state}

      {:ok, pending} ->
        expected = %{request_id: request_id, opcode: pending.opcode, lane_id: pending.lane_id}
        actual = %{request_id: request_id, opcode: opcode, lane_id: lane_id}

        case ResponseIdentity.validate(expected, actual) do
          :ok -> process_correlated_frame(state, request_id, pending, flags, body)
          {:error, reason} -> {:stop, reason, state}
        end
    end
  end

  defp process_correlated_frame(state, request_id, %{phase: phase}, _flags, _body)
       when phase in [:decoding, :awaiting_delivery],
       do: {:stop, {:duplicate_response, request_id}, state}

  defp process_correlated_frame(
         state,
         request_id,
         %{phase: :discarding} = pending,
         flags,
         body
       ),
       do: ConnectionDiscardedResponse.consume(state, request_id, pending, flags, body)

  defp process_correlated_frame(state, request_id, pending, flags, body),
    do: process_pending_frame(state, request_id, pending, flags, body)

  defp process_pending_frame(state, request_id, pending, flags, body) do
    if Codec.more_chunks?(flags),
      do: append_response_chunk(state, request_id, pending, flags, body),
      else: complete_response(state, request_id, pending, flags, body)
  end

  defp append_response_chunk(state, request_id, pending, flags, body) do
    case ResponseAssembler.append(
           pending,
           flags,
           body,
           state.response_chunk_bytes,
           state.response_chunk_frames,
           max_response_bytes: state.max_response_bytes,
           max_buffer_bytes: state.max_response_buffer_bytes,
           max_buffer_frames: state.max_response_chunk_frames
         ) do
      {:ok, pending, response_chunk_bytes, response_chunk_frames} ->
        {:ok,
         %{
           state
           | pending: Map.put(state.pending, request_id, pending),
             response_chunk_bytes: response_chunk_bytes,
             response_chunk_frames: response_chunk_frames
         }}

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  defp complete_response(state, request_id, pending, flags, body) do
    case ResponseAssembler.complete_parts(
           pending,
           flags,
           body,
           state.response_chunk_bytes,
           state.response_chunk_frames,
           max_response_bytes: state.max_response_bytes,
           max_buffer_bytes: state.max_response_buffer_bytes,
           max_buffer_frames: state.max_response_chunk_frames
         ) do
      {:error, reason} ->
        {:stop, reason, state}

      {:ok, logical_flags, logical_body} ->
        ConnectionResponseRuntime.finish(
          state,
          request_id,
          pending,
          logical_flags,
          logical_body
        )
    end
  end

  defp process_server_frame(state, lane_id, opcode, flags, body) do
    case ServerFramePolicy.classify(lane_id, opcode) do
      {:ok, kind} ->
        key = {lane_id, opcode}

        if Codec.more_chunks?(flags),
          do: append_server_frame(state, key, flags, body),
          else: complete_server_frame(state, key, kind, opcode, flags, body)

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  defp append_server_frame(state, key, flags, body) do
    case ServerFrameAssembler.append(state.server_frame_assembler, key, flags, body) do
      {:ok, assembler} -> {:ok, %{state | server_frame_assembler: assembler}}
      {:error, reason} -> {:stop, reason, state}
    end
  end

  defp complete_server_frame(state, key, kind, opcode, flags, body) do
    case ServerFrameAssembler.complete_parts(state.server_frame_assembler, key, flags, body) do
      {:error, reason, assembler} ->
        {:stop, reason, %{state | server_frame_assembler: assembler}}

      {:ok, logical_flags, logical_body, assembler} ->
        state = %{state | server_frame_assembler: assembler}

        ConnectionServerFrameRuntime.begin(
          state,
          kind,
          opcode,
          logical_flags,
          logical_body
        )
    end
  end
end
