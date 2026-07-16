defmodule FerricStore.SDK.Native.ConnectionServerFrameRuntime do
  @moduledoc false

  alias FerricStore.SDK.Native.{ConnectionDrain, ConnectionRequest, ConnectionServerFrameDecoder}

  alias FerricStore.Transport.SessionPolicy

  @spec begin(map(), term(), non_neg_integer(), non_neg_integer(), iodata()) ::
          {:ok, map()} | {:stop, term(), map()}
  def begin(%{decode: decode} = state, _kind, _opcode, _flags, _body)
      when not is_nil(decode),
      do: {:stop, :concurrent_frame_decode, state}

  def begin(state, kind, opcode, flags, body) do
    decode_token = make_ref()

    worker =
      ConnectionServerFrameDecoder.start(self(), decode_token, %{
        kind: kind,
        opcode: opcode,
        flags: flags,
        body: body,
        max_response_bytes: state.max_response_bytes,
        event_handler: state.event_handler
      })

    {:ok,
     Map.put(
       state,
       :decode,
       {:server,
        %{
          worker: worker,
          token: decode_token,
          opcode: opcode
        }}
     )}
  end

  @spec complete(map(), pid(), reference(), term()) ::
          {:noreply, map()} | {:stop, term(), map()}
  def complete(
        %{decode: {:server, %{worker: worker, token: token} = decode}} = state,
        worker,
        token,
        :deliver
      ) do
    :ok = ConnectionServerFrameDecoder.deliver(worker, token)
    decode = Map.put(decode, :phase, :delivering)
    {:noreply, %{state | decode: {:server, decode}}}
  end

  def complete(
        %{decode: {:server, %{worker: worker, token: token}}} = state,
        worker,
        token,
        {:stop, reason}
      ) do
    state = clear_decode(state)
    {:stop, reason, ConnectionRequest.fail_pending(state, reason)}
  end

  def complete(state, _worker, _token, _metadata), do: {:noreply, state}

  @spec delivered(map(), pid(), reference(), term()) ::
          {:noreply, map()} | {:stop, term(), map()}
  def delivered(
        %{
          decode:
            {:server,
             %{
               worker: worker,
               token: token,
               opcode: opcode,
               phase: :delivering
             }}
        } = state,
        worker,
        token,
        :ok
      ) do
    state = state |> clear_decode() |> maybe_begin_drain(opcode)
    continue_frames(state)
    {:noreply, state}
  end

  def delivered(
        %{decode: {:server, %{worker: worker, token: token}}} = state,
        worker,
        token,
        {:error, reason}
      ) do
    state = clear_decode(state)
    {:stop, reason, ConnectionRequest.fail_pending(state, reason)}
  end

  def delivered(state, _worker, _token, _outcome), do: {:noreply, state}

  defp clear_decode(state), do: Map.put(state, :decode, nil)

  defp maybe_begin_drain(state, opcode) do
    if SessionPolicy.server_frame_action(opcode) == :drain,
      do: ConnectionDrain.begin(state),
      else: state
  end

  defp continue_frames(%{drain: %{active: true}, pending: pending}) when map_size(pending) == 0,
    do: :ok

  defp continue_frames(_state), do: send(self(), :continue_frames)
end
