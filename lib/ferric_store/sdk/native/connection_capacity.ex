defmodule FerricStore.SDK.Native.ConnectionCapacity do
  @moduledoc false

  @unbounded %{max_in_flight: :infinity, max_in_flight_per_lane: :infinity}

  defstruct limits: %{}, lanes: %{}

  @type limit :: non_neg_integer() | :infinity
  @type limits :: %{
          required(:max_in_flight) => limit(),
          required(:max_in_flight_per_lane) => limit()
        }
  @type t :: %__MODULE__{
          limits: %{optional(pid()) => limits()},
          lanes: %{optional(pid()) => %{optional(non_neg_integer()) => non_neg_integer()}}
        }

  @spec put(t(), pid(), map()) :: t()
  def put(%__MODULE__{} = capacity, connection, limits) when is_pid(connection) do
    %{capacity | limits: Map.put(capacity.limits, connection, normalize(limits))}
  end

  @spec delete(t(), pid()) :: t()
  def delete(%__MODULE__{} = capacity, connection) when is_pid(connection) do
    %{
      capacity
      | limits: Map.delete(capacity.limits, connection),
        lanes: Map.delete(capacity.lanes, connection)
    }
  end

  @spec available?(t(), pid(), non_neg_integer(), non_neg_integer()) :: boolean()
  def available?(%__MODULE__{} = capacity, connection, total, lane_id)
      when is_pid(connection) and is_integer(total) and total >= 0 and
             is_integer(lane_id) and lane_id >= 0 do
    limits = Map.get(capacity.limits, connection, @unbounded)
    lane_count = capacity.lanes |> Map.get(connection, %{}) |> Map.get(lane_id, 0)

    below?(total, limits.max_in_flight) and
      below?(lane_count, limits.max_in_flight_per_lane)
  end

  @spec reserve(t(), pid(), non_neg_integer()) :: t()
  def reserve(%__MODULE__{} = capacity, connection, lane_id)
      when is_pid(connection) and is_integer(lane_id) and lane_id >= 0 do
    lanes =
      Map.update(capacity.lanes, connection, %{lane_id => 1}, fn counts ->
        Map.update(counts, lane_id, 1, &(&1 + 1))
      end)

    %{capacity | lanes: lanes}
  end

  @spec release(t(), pid(), non_neg_integer()) :: t()
  def release(%__MODULE__{} = capacity, connection, lane_id)
      when is_pid(connection) and is_integer(lane_id) and lane_id >= 0 do
    lanes =
      case Map.get(capacity.lanes, connection) do
        nil ->
          capacity.lanes

        counts ->
          counts = decrement(counts, lane_id)

          if map_size(counts) == 0,
            do: Map.delete(capacity.lanes, connection),
            else: Map.put(capacity.lanes, connection, counts)
      end

    %{capacity | lanes: lanes}
  end

  @spec normalize(map()) :: limits()
  def normalize(limits) when is_map(limits) do
    %{
      max_in_flight: limit(limits, :max_in_flight),
      max_in_flight_per_lane: limit(limits, :max_in_flight_per_lane)
    }
  end

  defp limit(limits, key) do
    case Map.get(limits, key, Map.get(limits, Atom.to_string(key), :infinity)) do
      value when is_integer(value) and value >= 0 -> value
      :infinity -> :infinity
      _invalid -> :infinity
    end
  end

  defp below?(_count, :infinity), do: true
  defp below?(count, limit), do: count < limit

  defp decrement(counts, lane_id) do
    case Map.get(counts, lane_id, 0) do
      count when count <= 1 -> Map.delete(counts, lane_id)
      count -> Map.put(counts, lane_id, count - 1)
    end
  end
end
