defmodule FerricStore.SDK.Native.RouteTarget do
  @moduledoc false

  alias FerricStore.{RouteKey, RoutingSlot}

  @max_slot 1_023
  @type t :: binary() | {:slot, 0..1023}

  @spec validate(term()) :: {:ok, t()} | {:error, term()}
  def validate({:slot, slot} = target) when slot in 0..@max_slot, do: {:ok, target}
  def validate(target), do: RouteKey.validate(target)

  @spec slot(t()) :: non_neg_integer()
  def slot({:slot, slot}) when slot in 0..@max_slot, do: slot
  def slot(key) when is_binary(key), do: RoutingSlot.for_key(key)
end
