defmodule FerricStore.SDK.Native.ConnectionEncodingWorker do
  @moduledoc false

  alias FerricStore.{FailureFormatter, Protocol}
  alias FerricStore.Protocol.ResponsePlan

  alias FerricStore.SDK.Native.{ConnectionEncodingTask, ConnectionTimers, PipelinePreparer}
  alias FerricStore.Transport.{RequestEncoder, SessionPolicy, Socket}

  @spec start(pid()) :: pid()
  def start(owner) when is_pid(owner), do: spawn_link(fn -> init(owner) end)

  defp init(owner), do: loop(owner, Process.monitor(owner))

  defp loop(owner, monitor) do
    receive do
      {:encode, job} ->
        handle_job(owner, monitor, job)

      {:DOWN, ^monitor, :process, ^owner, _reason} ->
        :ok

      :stop ->
        Process.demonitor(monitor, [:flush])
        :ok
    end
  end

  defp handle_job(owner, monitor, job) do
    case ConnectionEncodingTask.run(owner, monitor, job, &encode/1) do
      {:ok, frame, response_context} ->
        notify_owner(owner, job, {:ready, response_context})
        await_send_decision(owner, monitor, job, frame)

      {:error, _reason} = error ->
        notify_owner(owner, job, error)
        loop(owner, monitor)

      result when result in [:owner_down, :stop] ->
        Process.demonitor(monitor, [:flush])
        :ok
    end
  end

  defp await_send_decision(owner, monitor, job, frame) do
    receive do
      {:authorize_send, request_id, encode_token}
      when request_id == job.request_id and encode_token == job.encode_token ->
        notify_owner(owner, job, send_frame(job, frame))
        loop(owner, monitor)

      {:discard, request_id, encode_token}
      when request_id == job.request_id and encode_token == job.encode_token ->
        loop(owner, monitor)

      {:DOWN, ^monitor, :process, ^owner, _reason} ->
        :ok

      :stop ->
        Process.demonitor(monitor, [:flush])
        :ok
    end
  end

  defp encode(job) do
    with {:ok, remaining} <- remaining(job),
         {:ok, payload} <-
           PipelinePreparer.prepare(job.opcode, job.payload, job.max_pipeline_commands),
         response_context = %{
           response_plan: ResponsePlan.build(job.opcode, payload),
           compact_codec: job.compact_response_codec
         },
         payload <-
           payload
           |> Protocol.payload_or_empty()
           |> SessionPolicy.put_deadline(job.opcode, remaining),
         {:ok, frame} <-
           RequestEncoder.encode(
             job.opcode,
             job.lane_id,
             job.request_id,
             payload,
             job.max_request_bytes
           ),
         {:ok, _remaining} <- remaining(job) do
      {:ok, frame, response_context}
    else
      {:error, reason} -> {:error, reason}
    end
  rescue
    error ->
      {:error,
       {:encode_failed, FailureFormatter.exception_message(error, "request encoding failed")}}
  catch
    kind, reason -> {:error, {:encode_failed, FailureFormatter.inspect_term({kind, reason})}}
  end

  defp send_frame(job, frame) do
    with {:ok, _remaining} <- remaining(job) do
      case Socket.send(job.transport, job.socket, frame) do
        :ok -> :ok
        {:error, reason} -> {:transport_error, reason}
      end
    end
  rescue
    error ->
      {:transport_error,
       {:send_failed, FailureFormatter.exception_message(error, "transport send failed")}}
  catch
    kind, reason ->
      {:transport_error, {:send_failed, FailureFormatter.inspect_term({kind, reason})}}
  end

  defp notify_owner(owner, job, result) do
    send(
      owner,
      {:ferricstore_request_encoded, self(), job.request_id, job.encode_token, result}
    )
  end

  defp remaining(job) do
    case ConnectionTimers.remaining(job.deadline, job.timeout) do
      0 -> {:error, :timeout}
      timeout -> {:ok, timeout}
    end
  end
end
