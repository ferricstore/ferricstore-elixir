defmodule FerricStore.SDK.Native.ConnectionOptions do
  @moduledoc false

  alias FerricStore.SDK.Native.ConnectionOptionValidator

  @keys [
    :server_name,
    :verify,
    :tls_verify,
    :cacertfile,
    :cacerts,
    :connect_timeout,
    :send_timeout,
    :max_frame_bytes,
    :max_request_bytes,
    :max_response_bytes,
    :max_response_buffer_bytes,
    :max_in_flight,
    :max_in_flight_per_lane,
    :max_server_chunk_streams,
    :max_server_chunk_bytes,
    :server_chunk_timeout,
    :drain_timeout,
    :heartbeat_interval,
    :heartbeat_timeout
  ]

  @default_connect_timeout 5_000
  @default_send_timeout 5_000
  @default_max_frame_bytes 16 * 1024 * 1024
  @default_max_request_bytes 16 * 1024 * 1024
  @default_max_response_bytes 64 * 1024 * 1024
  @default_max_in_flight 256
  @default_max_in_flight_per_lane 256
  @default_max_server_chunk_streams 64
  @default_server_chunk_timeout 30_000
  @default_drain_timeout 5_000
  @default_heartbeat_interval 30_000
  @default_heartbeat_timeout 5_000
  @spec keys() :: [atom()]
  def keys, do: @keys

  @spec validate(map() | keyword()) :: :ok | {:error, {atom(), term()}}
  def validate(options) when is_map(options) or is_list(options) do
    Enum.reduce_while(@keys, :ok, fn key, :ok ->
      value = option(options, key)

      if ConnectionOptionValidator.valid?(key, value) do
        {:cont, :ok}
      else
        {:halt, {:error, {key, value}}}
      end
    end)
  end

  @spec effective(map() | keyword()) :: map()
  def effective(options) when is_map(options) or is_list(options) do
    max_response_bytes =
      positive_value(options, :max_response_bytes, @default_max_response_bytes)

    %{
      connect_timeout: timeout_value(options, :connect_timeout, @default_connect_timeout),
      send_timeout: finite_timeout_value(options, :send_timeout, @default_send_timeout),
      max_frame_bytes: positive_value(options, :max_frame_bytes, @default_max_frame_bytes),
      max_request_bytes: max_request_bytes(options),
      max_response_bytes: max_response_bytes,
      max_response_buffer_bytes:
        positive_value(options, :max_response_buffer_bytes, max_response_bytes),
      max_in_flight: positive_value(options, :max_in_flight, @default_max_in_flight),
      max_in_flight_per_lane:
        positive_value(
          options,
          :max_in_flight_per_lane,
          @default_max_in_flight_per_lane
        ),
      max_server_chunk_streams:
        positive_value(
          options,
          :max_server_chunk_streams,
          @default_max_server_chunk_streams
        ),
      max_server_chunk_bytes:
        positive_value(options, :max_server_chunk_bytes, max_response_bytes),
      server_chunk_timeout:
        timeout_value(options, :server_chunk_timeout, @default_server_chunk_timeout),
      drain_timeout: finite_timeout_value(options, :drain_timeout, @default_drain_timeout),
      heartbeat_interval:
        positive_timeout_value(options, :heartbeat_interval, @default_heartbeat_interval),
      heartbeat_timeout: timeout_value(options, :heartbeat_timeout, @default_heartbeat_timeout)
    }
  end

  @spec max_request_bytes(map() | keyword()) :: pos_integer()
  def max_request_bytes(options) when is_map(options) or is_list(options),
    do: positive_value(options, :max_request_bytes, @default_max_request_bytes)

  @spec identity(map() | keyword()) :: tuple()
  def identity(options) do
    policy = effective(options)

    {:connection_policy, policy.send_timeout, policy.max_frame_bytes, policy.max_request_bytes,
     policy.max_response_bytes, policy.max_response_buffer_bytes, policy.max_in_flight,
     policy.max_in_flight_per_lane, policy.max_server_chunk_streams,
     policy.max_server_chunk_bytes, policy.server_chunk_timeout, policy.heartbeat_interval,
     policy.heartbeat_timeout, policy.drain_timeout}
  end

  defp positive_value(options, key, default) do
    case option(options, key) do
      value when is_integer(value) and value > 0 -> value
      _missing_or_invalid -> default
    end
  end

  defp timeout_value(options, key, default) do
    case option(options, key) do
      :infinity -> :infinity
      value when is_integer(value) and value >= 0 -> value
      _missing_or_invalid -> default
    end
  end

  defp positive_timeout_value(options, key, default) do
    case option(options, key) do
      :infinity -> :infinity
      value when is_integer(value) and value > 0 -> value
      _missing_or_invalid -> default
    end
  end

  defp finite_timeout_value(options, key, default) do
    case option(options, key) do
      value when is_integer(value) and value >= 0 -> value
      _missing_or_invalid -> default
    end
  end

  defp option(options, key) when is_list(options), do: Keyword.get(options, key)

  defp option(options, key) when is_map(options),
    do: Map.get(options, key, Map.get(options, Atom.to_string(key)))
end
