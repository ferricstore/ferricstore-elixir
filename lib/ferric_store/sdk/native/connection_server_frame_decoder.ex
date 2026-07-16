defmodule FerricStore.SDK.Native.ConnectionServerFrameDecoder do
  @moduledoc false

  alias FerricStore.FailureFormatter
  alias FerricStore.SDK.Native.{Codec, ConnectionEventHandler}
  alias FerricStore.Transport.ServerFramePolicy

  @message_tag :ferricstore_server_frame_decoded
  @delivery_tag :ferricstore_deliver_server_frame
  @delivered_tag :ferricstore_server_frame_delivered
  @max_error_metadata_bytes 4_096

  @spec start(pid(), reference(), map()) :: pid()
  def start(owner, decode_token, frame) do
    spawn_link(fn ->
      owner_monitor = Process.monitor(owner)
      resolution = decode(frame)
      send(owner, {@message_tag, self(), decode_token, metadata(resolution)})
      await_delivery(owner, owner_monitor, decode_token, frame, resolution)
    end)
  end

  @spec deliver(pid(), reference()) :: :ok
  def deliver(worker, decode_token) do
    send(worker, {@delivery_tag, self(), decode_token})
    :ok
  end

  @spec stop({:server, map()} | map() | nil) :: :ok
  def stop({:server, decode}), do: stop(decode)

  def stop(%{worker: worker}) when is_pid(worker) do
    Process.unlink(worker)
    Process.exit(worker, :kill)
    :ok
  end

  def stop(_decode), do: :ok

  defp decode(frame) do
    body = IO.iodata_to_binary(frame.body)

    frame.opcode
    |> Codec.decode_response_envelope(frame.flags, body, frame.max_response_bytes)
    |> then(&ServerFramePolicy.resolve(frame.kind, &1))
  rescue
    error ->
      {:stop,
       {:invalid_server_frame_payload,
        {:decode_failed,
         FailureFormatter.exception_message(error, "server frame decoding failed")}}}
  catch
    kind, reason ->
      {:stop,
       {:invalid_server_frame_payload,
        {:decode_failed, FailureFormatter.inspect_term({kind, reason})}}}
  end

  defp metadata({:deliver, _value}), do: :deliver
  defp metadata({:stop, reason}), do: {:stop, compact_reason(reason)}

  defp compact_reason(reason) do
    if :erlang.external_size(reason) <= @max_error_metadata_bytes,
      do: reason,
      else: {:server_frame_error, FailureFormatter.inspect_term(reason)}
  end

  defp await_delivery(owner, owner_monitor, decode_token, frame, {:deliver, value}) do
    receive do
      {@delivery_tag, ^owner, ^decode_token} ->
        outcome = deliver(frame, owner, value)
        send(owner, {@delivered_tag, self(), decode_token, outcome})

      {:DOWN, ^owner_monitor, :process, ^owner, _reason} ->
        :ok
    end
  end

  defp await_delivery(_owner, _owner_monitor, _decode_token, _frame, {:stop, _reason}), do: :ok

  defp deliver(frame, owner, value) do
    ConnectionEventHandler.deliver(frame.event_handler, owner, frame.opcode, value)
    :ok
  rescue
    error ->
      {:error,
       {:server_frame_delivery_failed,
        FailureFormatter.exception_message(error, "server frame delivery failed")}}
  catch
    kind, reason ->
      {:error, {:server_frame_delivery_failed, FailureFormatter.inspect_term({kind, reason})}}
  end
end
