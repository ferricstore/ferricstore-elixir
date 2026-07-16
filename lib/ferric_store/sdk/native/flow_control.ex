defmodule FerricStore.SDK.Native.FlowControl do
  @moduledoc false

  alias FerricStore.Protocol.CommandSpec

  @default_max_pipeline_commands 1_024
  @client_max_pipeline_commands 100_000
  @window_update_opcode CommandSpec.fetch!(:window_update).opcode

  @spec default_max_pipeline_commands() :: pos_integer()
  def default_max_pipeline_commands, do: @default_max_pipeline_commands

  @spec increment(map(), map()) :: map()
  def increment(state, %{flow_controlled?: false}), do: state

  def increment(state, %{flow_controlled?: true, lane_id: lane_id}) do
    %{
      state
      | data_in_flight: state.data_in_flight + 1,
        pending_lanes: Map.update(state.pending_lanes, lane_id, 1, &(&1 + 1))
    }
  end

  @spec decrement(map(), map()) :: map()
  def decrement(state, %{flow_controlled?: false}), do: state

  def decrement(state, %{flow_controlled?: true, lane_id: lane_id}) do
    %{
      state
      | data_in_flight: state.data_in_flight - 1,
        pending_lanes: decrement_lane(state.pending_lanes, lane_id)
    }
  end

  @spec apply_server_capabilities(map(), term()) :: map()
  def apply_server_capabilities(state, startup) do
    capabilities = map_option(startup, :capabilities, %{})
    flow_control = map_option(capabilities, :flow_control, %{})
    limits = map_option(capabilities, :limits, %{})

    %{
      state
      | max_request_bytes:
          cap_positive_limit(
            state.configured_max_request_bytes,
            advertised_frame_bytes(startup, limits)
          ),
        max_in_flight:
          cap_limit(
            state.configured_max_in_flight,
            map_option(flow_control, :max_inflight_per_connection)
          ),
        max_in_flight_per_lane:
          cap_limit(
            state.configured_max_in_flight_per_lane,
            map_option(flow_control, :max_inflight_per_lane)
          ),
        max_pipeline_commands: pipeline_limit(map_option(limits, :max_pipeline_commands))
    }
  end

  @spec apply_window_update(map(), non_neg_integer(), term()) :: map()
  def apply_window_update(state, opcode, result) do
    apply_window_limits(state, response_window_limits(opcode, result))
  end

  @spec response_window_limits(non_neg_integer(), term()) ::
          :none
          | {:window_limits, %{max_in_flight: term(), max_in_flight_per_lane: term()}}
  def response_window_limits(@window_update_opcode, {:ok, acknowledgement}) do
    with true <- map_option(acknowledgement, :accepted, false),
         limits when is_map(limits) <- map_option(acknowledgement, :limits) do
      {:window_limits,
       %{
         max_in_flight: map_option(limits, :max_inflight_per_connection),
         max_in_flight_per_lane: map_option(limits, :max_inflight_per_lane)
       }}
    else
      _invalid_acknowledgement -> :none
    end
  end

  def response_window_limits(_opcode, _result), do: :none

  @spec apply_window_limits(map(), :none | {:window_limits, map()}) :: map()
  def apply_window_limits(state, :none), do: state

  def apply_window_limits(state, {:window_limits, limits}) do
    %{
      state
      | max_in_flight:
          window_limit(
            limits,
            :max_in_flight,
            state.configured_max_in_flight,
            state.max_in_flight
          ),
        max_in_flight_per_lane:
          window_limit(
            limits,
            :max_in_flight_per_lane,
            state.configured_max_in_flight_per_lane,
            state.max_in_flight_per_lane
          )
    }
  end

  defp decrement_lane(pending_lanes, lane_id) do
    case Map.get(pending_lanes, lane_id, 0) do
      count when count <= 1 -> Map.delete(pending_lanes, lane_id)
      count -> Map.put(pending_lanes, lane_id, count - 1)
    end
  end

  defp pipeline_limit(advertised) when is_integer(advertised) and advertised >= 0,
    do: min(advertised, @client_max_pipeline_commands)

  defp pipeline_limit(_advertised), do: @default_max_pipeline_commands

  defp advertised_frame_bytes(startup, limits) do
    case map_option(limits, :max_frame_bytes) do
      value when is_integer(value) and value > 0 -> value
      _missing_or_invalid -> map_option(startup, :max_frame_bytes)
    end
  end

  defp cap_positive_limit(configured, advertised)
       when is_integer(advertised) and advertised > 0,
       do: min(configured, advertised)

  defp cap_positive_limit(configured, _advertised), do: configured

  defp cap_limit(configured, advertised)
       when is_integer(advertised) and advertised >= 0,
       do: min(configured, advertised)

  defp cap_limit(configured, _advertised), do: configured

  defp window_limit(limits, key, configured, current) do
    case map_option(limits, key) do
      advertised when is_integer(advertised) and advertised >= 0 ->
        min(configured, advertised)

      _missing_or_invalid ->
        current
    end
  end

  defp map_option(map, key, default \\ nil)

  defp map_option(map, key, default) when is_map(map),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))

  defp map_option(_value, _key, default), do: default
end
