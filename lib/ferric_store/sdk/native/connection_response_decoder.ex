defmodule FerricStore.SDK.Native.ConnectionResponseDecoder do
  @moduledoc false

  alias FerricStore.{FailureFormatter, SDK.Native.Codec}
  alias FerricStore.SDK.Native.FlowControl

  @message_tag :ferricstore_response_decoded
  @delivery_tag :ferricstore_deliver_decoded_response

  @spec start(pid(), non_neg_integer(), reference(), map()) :: pid()
  def start(owner, request_id, decode_token, response) do
    spawn_link(fn ->
      owner_monitor = Process.monitor(owner)
      result = decode(response)
      metadata = metadata(response.opcode, response.target, result)
      send(owner, {@message_tag, self(), request_id, decode_token, metadata})
      await_delivery(owner, owner_monitor, request_id, decode_token, response.target, result)
    end)
  end

  @spec deliver(pid(), non_neg_integer(), reference()) :: :ok
  def deliver(worker, request_id, decode_token) do
    Process.unlink(worker)
    send(worker, {@delivery_tag, self(), request_id, decode_token})
    :ok
  end

  @spec stop(map()) :: :ok
  def stop(%{phase: :decoding, decode_worker: worker}) when is_pid(worker) do
    Process.unlink(worker)
    Process.exit(worker, :kill)
    :ok
  end

  def stop(_pending), do: :ok

  @spec stop_pending(map()) :: :ok
  def stop_pending(pending) do
    Enum.each(pending, fn {_request_id, request} -> stop(request) end)
  end

  defp decode(response) do
    body = IO.iodata_to_binary(response.body)

    case Codec.decode_response(
           response.opcode,
           response.flags,
           body,
           response.max_response_bytes,
           response.response_context
         ) do
      {:ok, value} -> {:ok, value}
      {:error, reason} -> {:error, reason}
      {status, value} -> {:error, {status, value}}
    end
  rescue
    error ->
      {:error,
       {:decode_failed, FailureFormatter.exception_message(error, "response decoding failed")}}
  catch
    kind, reason ->
      {:error, {:decode_failed, FailureFormatter.inspect_term({kind, reason})}}
  end

  defp metadata(_opcode, :heartbeat, {:ok, _value}), do: {:heartbeat, :ok}

  defp metadata(_opcode, :heartbeat, {:error, reason}),
    do: {:heartbeat, {:error, compact_heartbeat_error(reason)}}

  defp metadata(opcode, _target, result),
    do: {:response, FlowControl.response_window_limits(opcode, result)}

  defp compact_heartbeat_error(reason) when is_atom(reason), do: reason

  defp compact_heartbeat_error(reason),
    do: {:invalid_heartbeat_response, FailureFormatter.inspect_term(reason)}

  defp await_delivery(owner, owner_monitor, request_id, decode_token, target, result) do
    receive do
      {@delivery_tag, ^owner, ^request_id, ^decode_token} ->
        reply(target, owner, request_id, decode_token, result)

      {:DOWN, ^owner_monitor, :process, ^owner, _reason} ->
        :ok
    end
  end

  defp reply({:call, from}, _owner, _request_id, _decode_token, result),
    do: GenServer.reply(from, result)

  defp reply({:message, reply_to, tag}, owner, _request_id, _decode_token, result),
    do: send(reply_to, {:ferricstore_connection_response, owner, tag, result})

  defp reply(
         {:acknowledged_message, reply_to, tag},
         owner,
         _request_id,
         decode_token,
         result
       ) do
    send(
      reply_to,
      {:ferricstore_connection_response, owner, tag, result, decode_token}
    )
  end

  defp reply(:heartbeat, _owner, _request_id, _decode_token, _result), do: :ok
end
