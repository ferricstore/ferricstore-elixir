defmodule FerricStore.SDK.Native.Connection do
  @moduledoc false

  use GenServer

  alias FerricStore.SDK.Native.{
    ConnectionAsyncClient,
    ConnectionCallRuntime,
    ConnectionCastRuntime,
    ConnectionClient,
    ConnectionInfoRuntime,
    ConnectionInitializer,
    ConnectionShutdown,
    ConnectionTermination,
    FlowControl
  }

  alias FerricStore.Transport.{FrameLimits, FrameStream, ServerFrameAssembler}

  @default_timeout 5_000
  @default_max_pipeline_commands FlowControl.default_max_pipeline_commands()
  @max_response_chunk_frames FrameLimits.max_response_chunk_frames()

  @derive {Inspect, except: [:endpoint, :buffer, :pending, :server_frame_assembler]}
  defstruct [
    :socket,
    :transport,
    :endpoint,
    :encoder,
    :event_handler,
    next_request_id: 1,
    buffer: %FrameStream{},
    pending: %{},
    pending_targets: %{},
    pending_lanes: %{},
    data_in_flight: 0,
    response_chunk_bytes: 0,
    response_chunk_frames: 0,
    decode: nil,
    server_frame_assembler: %ServerFrameAssembler{},
    max_frame_bytes: 16 * 1024 * 1024,
    configured_max_request_bytes: 16 * 1024 * 1024,
    max_request_bytes: 16 * 1024 * 1024,
    max_response_bytes: 64 * 1024 * 1024,
    max_response_buffer_bytes: 64 * 1024 * 1024,
    max_response_chunk_frames: @max_response_chunk_frames,
    configured_max_in_flight: 256,
    configured_max_in_flight_per_lane: 256,
    max_in_flight: 256,
    max_in_flight_per_lane: 256,
    max_pipeline_commands: @default_max_pipeline_commands,
    drain: %{active: false, timeout: 5_000, timer: nil, token: nil},
    heartbeat_interval: 30_000,
    heartbeat_timeout: 5_000,
    heartbeat_timer: nil,
    heartbeat_token: nil
  ]

  @spec start_link(map()) :: GenServer.on_start()
  def start_link(endpoint), do: GenServer.start_link(__MODULE__, endpoint)

  def child_spec(endpoint) do
    %{
      id: {__MODULE__, make_ref()},
      start: {__MODULE__, :start_link, [endpoint]},
      restart: :temporary,
      type: :worker
    }
  end

  @spec start(map()) :: GenServer.on_start()
  def start(endpoint), do: GenServer.start(__MODULE__, endpoint)

  def close(pid), do: ConnectionClient.close(pid)

  def request(pid, opcode, payload, lane_id, timeout \\ @default_timeout),
    do: ConnectionClient.request(pid, opcode, payload, lane_id, timeout)

  def complete_bootstrap(pid, startup, timeout \\ @default_timeout),
    do: ConnectionClient.complete_bootstrap(pid, startup, timeout)

  def capacity(pid, timeout \\ @default_timeout), do: ConnectionClient.capacity(pid, timeout)

  def async_request(pid, reply_to, tag, opcode, payload, lane_id, timeout \\ @default_timeout),
    do: ConnectionAsyncClient.request(pid, reply_to, tag, opcode, payload, lane_id, timeout)

  def acknowledged_async_request(pid, reply_to, tag, opcode, payload, lane_id, timeout),
    do:
      ConnectionAsyncClient.acknowledged_request(
        pid,
        reply_to,
        tag,
        opcode,
        payload,
        lane_id,
        timeout
      )

  def acknowledge_response(pid, reply_to, tag, delivery_token),
    do: ConnectionAsyncClient.acknowledge(pid, reply_to, tag, delivery_token)

  def cancel(pid, reply_to, tag, timeout \\ @default_timeout),
    do: ConnectionClient.cancel(pid, reply_to, tag, timeout)

  def cancel_async(pid, reply_to, tag), do: ConnectionAsyncClient.cancel(pid, reply_to, tag)
  def drain(pid), do: ConnectionClient.drain(pid)
  def abort(pid, reason), do: ConnectionClient.abort(pid, reason)

  @impl true
  def init(endpoint), do: ConnectionInitializer.run(endpoint, __MODULE__)

  @impl true
  def handle_call(request, from, state), do: ConnectionCallRuntime.handle(request, from, state)

  @impl true
  def handle_cast(request, state), do: ConnectionCastRuntime.handle(request, state)

  @impl true
  def handle_info(message, state),
    do: message |> ConnectionInfoRuntime.handle(state) |> ConnectionTermination.handle()

  @impl true
  def terminate(_reason, state), do: ConnectionShutdown.run(state)
end
