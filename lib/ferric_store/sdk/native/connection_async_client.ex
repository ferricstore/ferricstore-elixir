defmodule FerricStore.SDK.Native.ConnectionAsyncClient do
  @moduledoc false

  alias FerricStore.SDK.Native.ConnectionTimers

  @default_timeout 5_000

  @spec request(
          pid(),
          pid(),
          reference(),
          non_neg_integer(),
          term(),
          non_neg_integer(),
          timeout()
        ) ::
          :ok
  def request(pid, reply_to, tag, opcode, payload, lane_id, timeout \\ @default_timeout)
      when is_pid(reply_to) and is_reference(tag) do
    dispatch(pid, :message, reply_to, tag, opcode, payload, lane_id, timeout)
  end

  @spec acknowledged_request(
          pid(),
          pid(),
          reference(),
          non_neg_integer(),
          term(),
          non_neg_integer(),
          timeout()
        ) :: :ok
  def acknowledged_request(pid, reply_to, tag, opcode, payload, lane_id, timeout)
      when is_pid(reply_to) and is_reference(tag) do
    dispatch(pid, :acknowledged_message, reply_to, tag, opcode, payload, lane_id, timeout)
  end

  @spec acknowledge(pid(), pid(), reference(), reference()) :: :ok
  def acknowledge(pid, reply_to, tag, delivery_token)
      when is_pid(pid) and is_pid(reply_to) and is_reference(tag) and
             is_reference(delivery_token) do
    GenServer.cast(pid, {:acknowledge_response, reply_to, tag, delivery_token})
  end

  @spec cancel(pid(), pid(), reference()) :: :ok
  def cancel(pid, reply_to, tag)
      when is_pid(pid) and is_pid(reply_to) and is_reference(tag) do
    GenServer.cast(pid, {:cancel, reply_to, tag})
  end

  defp dispatch(pid, delivery, reply_to, tag, opcode, payload, lane_id, timeout) do
    deadline = ConnectionTimers.request_deadline(timeout)

    GenServer.cast(
      pid,
      {:async_request, delivery, reply_to, tag, opcode, payload, lane_id, timeout, deadline}
    )
  end
end
