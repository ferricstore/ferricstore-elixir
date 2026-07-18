defmodule FerricStore.FlowRouting.RouteSource do
  @moduledoc false

  alias FerricStore.{RouteKey, RoutingSlot}

  @auto_partition_prefix "__flow_auto__:"

  @spec claim_partition(term()) :: RouteKey.resolution()
  def claim_partition(value) when is_binary(value) do
    with {:ok, value} <- RouteKey.validate(value) do
      case String.upcase(value) do
        selector when selector in ["AUTO", "ANY"] -> :none
        "GLOBAL" -> slot_route("f")
        _partition -> logical_partition_route(value)
      end
    end
  end

  def claim_partition(value), do: invalid(value)

  @spec logical_partition(term()) :: RouteKey.resolution()
  def logical_partition(value) when is_binary(value) do
    with {:ok, value} <- RouteKey.validate(value),
         do: logical_partition_route(value)
  end

  def logical_partition(value), do: invalid(value)

  @spec auto_id(term()) :: RouteKey.resolution()
  def auto_id(value) when is_binary(value) do
    with {:ok, value} <- RouteKey.validate(value) do
      bucket = rem(:erlang.crc32(value), 256)
      slot_route("fa:#{bucket}")
    end
  end

  def auto_id(value), do: invalid(value)

  defp logical_partition_route(@auto_partition_prefix <> bucket = partition) do
    case canonical_auto_bucket(bucket) do
      {:ok, bucket} -> slot_route("fa:#{bucket}")
      :error -> hashed_partition_route(partition)
    end
  end

  defp logical_partition_route(partition), do: hashed_partition_route(partition)

  defp hashed_partition_route(partition) do
    digest = partition |> then(&:crypto.hash(:sha256, &1)) |> Base.url_encode64(padding: false)
    slot_route("f:#{digest}")
  end

  defp slot_route(tag), do: {:ok, {:slot, RoutingSlot.for_tag(tag)}}

  defp canonical_auto_bucket(bucket) do
    case Integer.parse(bucket) do
      {number, ""} when number in 0..255 ->
        if bucket == Integer.to_string(number), do: {:ok, number}, else: :error

      _invalid ->
        :error
    end
  end

  defp invalid(value), do: {:error, {:invalid_route_key, value}}
end
