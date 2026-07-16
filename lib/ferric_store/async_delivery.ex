defmodule FerricStore.AsyncDelivery do
  @moduledoc false

  @spec new() :: reference()
  def new, do: :erlang.alias()

  @spec deliver(reference(), module(), term()) :: :ok
  def deliver(ref, reply_module, result) when is_reference(ref) and is_atom(reply_module) do
    send(ref, {reply_module, ref, result})
    :ok
  end

  @spec deactivate(reference()) :: :ok
  def deactivate(ref) when is_reference(ref) do
    :erlang.unalias(ref)
    :ok
  end

  @spec drain(reference(), module()) :: :ok
  def drain(ref, reply_module) when is_reference(ref) and is_atom(reply_module) do
    receive do
      {^reply_module, ^ref, _result} -> drain(ref, reply_module)
    after
      0 -> :ok
    end
  end
end
