defmodule FerricStore.SDK.Native.CoordinatorReply do
  @moduledoc false

  alias FerricStore.{AsyncDelivery, AsyncRequest}

  @spec admit({:noreply, term()} | {:reply, term(), term()}, reference()) ::
          {:reply, :ok, term()}
  def admit({:noreply, state}, _ref), do: {:reply, :ok, state}

  def admit({:reply, result, state}, ref) do
    AsyncDelivery.deliver(ref, AsyncRequest, result)
    {:reply, :ok, state}
  end

  @spec reply(term(), term()) :: :ok
  def reply({:async, _caller, ref}, result),
    do: AsyncDelivery.deliver(ref, AsyncRequest, result)

  def reply(from, result), do: GenServer.reply(from, result)
end
