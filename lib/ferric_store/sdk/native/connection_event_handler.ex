defmodule FerricStore.SDK.Native.ConnectionEventHandler do
  @moduledoc false

  alias FerricStore.Transport.EventDispatcher

  @spec normalize(pid() | function() | term(), pid()) :: pid() | {:dispatcher, pid()} | term()
  def normalize(handler, owner) when is_function(handler, 1) and is_pid(owner),
    do: {:dispatcher, EventDispatcher.start(owner, handler)}

  def normalize(handler, _owner), do: handler

  @spec deliver(term(), pid(), non_neg_integer(), term()) :: term()
  def deliver(handler, connection, opcode, value) when is_pid(handler) do
    send(handler, {:ferricstore_server_frame, connection, opcode, value})
  end

  def deliver({:dispatcher, dispatcher}, connection, opcode, value) do
    result =
      EventDispatcher.dispatch(dispatcher, %{
        connection: connection,
        opcode: opcode,
        value: value
      })

    if result == :dropped, do: result, else: committed_result(dispatcher, result)
  end

  def deliver(_handler, _connection, _opcode, _value), do: :ok

  @spec capacity_changed(term(), pid(), map()) :: :ok
  def capacity_changed(handler, connection, capacity)
      when is_pid(handler) and is_pid(connection) and is_map(capacity) do
    send(handler, {:ferricstore_connection_capacity, connection, capacity})
    :ok
  end

  def capacity_changed(_handler, _connection, _capacity), do: :ok

  @spec stop(term()) :: :ok
  def stop({:dispatcher, dispatcher}), do: EventDispatcher.stop(dispatcher)
  def stop(_handler), do: :ok

  defp committed_result(dispatcher, result) do
    EventDispatcher.barrier(dispatcher)
    result
  end
end
