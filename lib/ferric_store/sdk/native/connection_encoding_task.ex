defmodule FerricStore.SDK.Native.ConnectionEncodingTask do
  @moduledoc false

  alias FerricStore.{FailureFormatter, SDK.Native.ConnectionTimers}

  @message_tag :ferricstore_encoding_task_result

  @spec run(pid(), reference(), map(), (map() -> term())) ::
          term() | :owner_down | :stop
  def run(owner, owner_monitor, job, encoder)
      when is_pid(owner) and is_reference(owner_monitor) and is_function(encoder, 1) do
    case remaining(job) do
      {:ok, timeout} -> run_with_timeout(owner, owner_monitor, job, encoder, timeout)
      {:error, :timeout} = timeout -> timeout
    end
  end

  defp run_with_timeout(owner, owner_monitor, job, encoder, timeout) do
    parent = self()
    token = make_ref()
    reply_alias = Process.alias()

    {guardian, monitor} =
      spawn_monitor(fn -> guard_encoding(parent, reply_alias, token, job, encoder) end)

    await(guardian, monitor, reply_alias, token, owner, owner_monitor, timeout)
  end

  defp await(guardian, monitor, reply_alias, token, owner, owner_monitor, :infinity) do
    receive do
      {^token, result} ->
        finish_result(monitor, reply_alias, result)

      {:DOWN, ^monitor, :process, ^guardian, reason} ->
        finish_failure(reply_alias, reason)

      {:DOWN, ^owner_monitor, :process, ^owner, _reason} ->
        cancel_guardian(guardian, monitor, reply_alias, token)
        :owner_down

      :stop ->
        cancel_guardian(guardian, monitor, reply_alias, token)
        :stop
    end
  end

  defp await(guardian, monitor, reply_alias, token, owner, owner_monitor, timeout) do
    receive do
      {^token, result} ->
        finish_result(monitor, reply_alias, result)

      {:DOWN, ^monitor, :process, ^guardian, reason} ->
        finish_failure(reply_alias, reason)

      {:DOWN, ^owner_monitor, :process, ^owner, _reason} ->
        cancel_guardian(guardian, monitor, reply_alias, token)
        :owner_down

      :stop ->
        cancel_guardian(guardian, monitor, reply_alias, token)
        :stop
    after
      timeout ->
        cancel_guardian(guardian, monitor, reply_alias, token)
        {:error, :timeout}
    end
  end

  defp finish_result(monitor, reply_alias, result) do
    Process.unalias(reply_alias)
    Process.demonitor(monitor, [:flush])
    result
  end

  defp finish_failure(reply_alias, reason) do
    Process.unalias(reply_alias)
    encode_failure(reason)
  end

  defp encode_failure(reason),
    do: {:error, {:encode_failed, FailureFormatter.inspect_term(reason)}}

  defp remaining(job) do
    case ConnectionTimers.remaining(job.deadline, job.timeout) do
      0 -> {:error, :timeout}
      timeout -> {:ok, timeout}
    end
  end

  defp cancel_guardian(guardian, monitor, reply_alias, token) do
    Process.unalias(reply_alias)
    send(guardian, {:cancel_encoding, self(), token})
    Process.demonitor(monitor, [:flush])
    :ok
  end

  defp guard_encoding(parent, reply_alias, token, job, encoder) do
    parent_monitor = Process.monitor(parent)
    guardian = self()

    worker =
      spawn_link(fn ->
        send(guardian, {@message_tag, self(), encoder.(job)})
      end)

    receive do
      {@message_tag, ^worker, result} ->
        Process.demonitor(parent_monitor, [:flush])
        send(reply_alias, {token, result})

      {:DOWN, ^parent_monitor, :process, ^parent, _reason} ->
        Process.exit(worker, :kill)

      {:cancel_encoding, ^parent, ^token} ->
        Process.demonitor(parent_monitor, [:flush])
        Process.exit(worker, :kill)
    end
  end
end
