defmodule FerricStore.Protocol.DecodeBudget do
  @moduledoc false

  alias FerricStore.RequestLimits

  @spec new() :: non_neg_integer()
  def new, do: RequestLimits.max_batch_items()

  @spec consume(non_neg_integer(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, :collection_too_large}
  def consume(remaining, count)
      when is_integer(remaining) and remaining >= 0 and is_integer(count) and count >= 0 do
    if count <= remaining,
      do: {:ok, remaining - count},
      else: {:error, :collection_too_large}
  end
end
