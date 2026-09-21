defmodule FerricStore.SDK.Native.ConnectionDiscardedControlResponse do
  @moduledoc false

  alias FerricStore.Protocol.CommandSpec

  alias FerricStore.SDK.Native.{
    Codec,
    ConnectionResponseRuntime
  }

  alias FerricStore.Transport.ResponseAssembler

  @window_update_opcode CommandSpec.fetch!(:window_update).opcode

  @spec authoritative?(map()) :: boolean()
  def authoritative?(%{opcode: @window_update_opcode}), do: true
  def authoritative?(_pending), do: false

  @spec consume(map(), non_neg_integer(), map(), non_neg_integer(), binary()) ::
          {:ok, map()} | {:stop, term(), map()}
  def consume(state, request_id, pending, flags, body) do
    opts = [
      max_response_bytes: state.max_response_bytes,
      max_buffer_bytes: state.max_response_buffer_bytes,
      max_buffer_frames: state.max_response_chunk_frames
    ]

    case response_parts(pending, flags, body, state, opts) do
      {:ok, pending, response_chunk_bytes, response_chunk_frames} ->
        {:ok,
         %{
           state
           | pending: Map.put(state.pending, request_id, pending),
             response_chunk_bytes: response_chunk_bytes,
             response_chunk_frames: response_chunk_frames
         }}

      {:ok, logical_flags, logical_body} ->
        ConnectionResponseRuntime.begin_discarded_decode(
          state,
          request_id,
          pending,
          logical_flags,
          logical_body,
          pending.chunk_bytes + byte_size(body)
        )

      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  @spec prepare_buffer(map(), map()) :: {map(), map()}
  def prepare_buffer(state, %{opcode: @window_update_opcode} = pending),
    do: {state, pending}

  def prepare_buffer(state, pending) do
    chunk_bytes = pending.chunk_bytes
    chunk_frames = pending.chunk_frames

    pending =
      Map.merge(pending, %{
        discarded_response_bytes: chunk_bytes,
        discarded_response_frames: chunk_frames,
        chunks: [],
        chunk_bytes: 0,
        chunk_frames: 0
      })

    state = %{
      state
      | response_chunk_bytes: max(state.response_chunk_bytes - chunk_bytes, 0),
        response_chunk_frames: max(state.response_chunk_frames - chunk_frames, 0)
    }

    {state, pending}
  end

  defp response_parts(pending, flags, body, state, opts) do
    if Codec.more_chunks?(flags) do
      ResponseAssembler.append(
        pending,
        flags,
        body,
        state.response_chunk_bytes,
        state.response_chunk_frames,
        opts
      )
    else
      ResponseAssembler.complete(
        pending,
        flags,
        body,
        state.response_chunk_bytes,
        state.response_chunk_frames,
        opts
      )
    end
  end
end
