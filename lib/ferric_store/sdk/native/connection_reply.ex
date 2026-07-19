defmodule FerricStore.SDK.Native.ConnectionReply do
  @moduledoc false

  @spec send(term(), term()) :: :ok | term()
  def send({:call, from}, result), do: GenServer.reply(from, result)

  def send({:message, reply_to, tag}, result),
    do: Kernel.send(reply_to, {:ferricstore_connection_response, self(), tag, result})

  def send({:acknowledged_message, reply_to, tag}, result),
    do: Kernel.send(reply_to, {:ferricstore_connection_response, self(), tag, result})

  def send(:heartbeat, _result), do: :ok
  def send(:discard, _result), do: :ok
end
