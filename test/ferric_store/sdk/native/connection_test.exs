defmodule FerricStore.SDK.Native.ConnectionTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.Codec
  alias FerricStore.SDK.Native.Connection
  alias FerricStore.Test.NativeServer
  alias FerricStore.Transport.{FrameStream, ServerFrameAssembler}

  test "one socket multiplexes concurrent requests" do
    {server, connection} =
      start_connection(
        response_fun: fn _request -> {:reply_after, 50, "OK"} end,
        max_in_flight: 32
      )

    started = System.monotonic_time(:millisecond)

    results =
      1..8
      |> Enum.map(fn index ->
        Task.async(fn ->
          Connection.request(connection, 0x0101, %{"key" => "key-#{index}"}, 1, 1_000)
        end)
      end)
      |> Task.await_many(2_000)

    elapsed = System.monotonic_time(:millisecond) - started

    assert results == List.duplicate({:ok, "OK"}, 8)
    assert elapsed < 220
    assert NativeServer.connection_count(server) == 1
  end

  test "large data encoding cannot block control requests in the socket owner" do
    {_server, connection} = start_connection(max_request_bytes: 16 * 1024 * 1024)

    items =
      Enum.map(1..100_000, fn index ->
        %{"a" => index, "b" => "value", "c" => [index, index]}
      end)

    tag = make_ref()

    assert :ok =
             Connection.async_request(
               connection,
               self(),
               tag,
               0x0101,
               %{"items" => items},
               1,
               60
             )

    started = System.monotonic_time(:millisecond)
    assert {:ok, "OK"} = Connection.request(connection, 0x0003, %{}, 0, 1_000)
    assert System.monotonic_time(:millisecond) - started < 120

    assert_receive {:ferricstore_connection_response, ^connection, ^tag, {:error, :timeout}}, 120
    refute_receive {:native_server_request, %{opcode: 0x0101}}, 500
  end

  test "in-flight admission is bounded without queuing commands past their deadlines" do
    {_server, connection} =
      start_connection(response_fun: fn _request -> :noreply end, max_in_flight: 2)

    first =
      Task.async(fn -> Connection.request(connection, 0x0101, %{"key" => "one"}, 1, 500) end)

    second =
      Task.async(fn -> Connection.request(connection, 0x0101, %{"key" => "two"}, 1, 500) end)

    assert_eventually(fn -> map_size(:sys.get_state(connection).pending) == 2 end)

    started = System.monotonic_time(:millisecond)

    assert {:error, :connection_backpressure} =
             Connection.request(connection, 0x0101, %{"key" => "three"}, 1, 500)

    assert System.monotonic_time(:millisecond) - started < 40
    assert Task.await(first, 1_000) == {:error, :timeout}
    assert Task.await(second, 1_000) == {:error, :timeout}

    assert_eventually(fn ->
      state = :sys.get_state(connection)

      state.data_in_flight == 2 and state.pending_targets == %{} and
        map_size(state.pending) == 2 and
        Enum.all?(state.pending, fn {_request_id, pending} ->
          pending.phase == :discarding
        end)
    end)
  end

  test "WINDOW_UPDATE acknowledgements replace active limits without losing client ceilings" do
    {_server, connection} =
      start_connection(
        max_in_flight: 10,
        max_in_flight_per_lane: 10,
        event_handler: self(),
        response_fun: fn
          %{opcode: 0x000D, payload: %{"phase" => "lower"}} ->
            %{
              "accepted" => true,
              "limits" => %{
                "max_inflight_per_connection" => 2,
                "max_inflight_per_lane" => 1
              }
            }

          %{opcode: 0x000D, payload: %{"phase" => "raise"}} ->
            %{
              "accepted" => true,
              "limits" => %{
                "max_inflight_per_connection" => 8,
                "max_inflight_per_lane" => 7
              }
            }

          %{opcode: 0x000D, payload: %{"phase" => "connection_only"}} ->
            %{
              "accepted" => true,
              "limits" => %{"max_inflight_per_connection" => 6}
            }

          %{opcode: 0x000D, payload: %{"phase" => "lane_only"}} ->
            %{
              "accepted" => true,
              "limits" => %{"max_inflight_per_lane" => 5}
            }

          %{opcode: 0x000D} ->
            %{"accepted" => false, "limits" => %{"max_inflight_per_connection" => 1}}

          _request ->
            "OK"
        end
      )

    assert :ok =
             Connection.complete_bootstrap(connection, %{
               "capabilities" => %{
                 "flow_control" => %{
                   "max_inflight_per_connection" => 4,
                   "max_inflight_per_lane" => 3
                 }
               }
             })

    assert %{max_in_flight: 4, max_in_flight_per_lane: 3} = :sys.get_state(connection)

    assert {:ok, %{"accepted" => true}} =
             Connection.request(connection, 0x000D, %{"phase" => "lower"}, 0, 1_000)

    assert %{max_in_flight: 2, max_in_flight_per_lane: 1} = :sys.get_state(connection)

    assert_receive {:ferricstore_connection_capacity, ^connection,
                    %{max_in_flight: 2, max_in_flight_per_lane: 1}}

    assert {:ok, %{"accepted" => true}} =
             Connection.request(connection, 0x000D, %{"phase" => "raise"}, 0, 1_000)

    assert %{max_in_flight: 8, max_in_flight_per_lane: 7} = :sys.get_state(connection)

    assert_receive {:ferricstore_connection_capacity, ^connection,
                    %{max_in_flight: 8, max_in_flight_per_lane: 7}}

    assert {:ok, %{"accepted" => true}} =
             Connection.request(connection, 0x000D, %{"phase" => "connection_only"}, 0, 1_000)

    assert %{max_in_flight: 6, max_in_flight_per_lane: 7} = :sys.get_state(connection)

    assert {:ok, %{"accepted" => true}} =
             Connection.request(connection, 0x000D, %{"phase" => "lane_only"}, 0, 1_000)

    assert %{max_in_flight: 6, max_in_flight_per_lane: 5} = :sys.get_state(connection)

    assert {:ok, %{"accepted" => false}} =
             Connection.request(connection, 0x000D, %{"phase" => "invalid"}, 0, 1_000)

    assert %{max_in_flight: 6, max_in_flight_per_lane: 5} = :sys.get_state(connection)
  end

  test "startup frame limits cap outbound requests without raising the client ceiling" do
    {_server, connection} = start_connection(max_request_bytes: 128)

    assert :ok =
             Connection.complete_bootstrap(connection, %{
               "capabilities" => %{"limits" => %{"max_frame_bytes" => 64}}
             })

    assert %{configured_max_request_bytes: 128, max_request_bytes: 64} =
             :sys.get_state(connection)

    assert {:error, :request_too_large} =
             Connection.request(
               connection,
               0x0102,
               %{"key" => "large", "value" => String.duplicate("x", 128)},
               1,
               200
             )

    refute_receive {:native_server_request, %{opcode: 0x0102}}, 100

    assert :ok =
             Connection.complete_bootstrap(connection, %{
               "capabilities" => %{"limits" => %{"max_frame_bytes" => 1_024}}
             })

    assert %{configured_max_request_bytes: 128, max_request_bytes: 128} =
             :sys.get_state(connection)
  end

  test "control requests reopen zero flow-control windows without consuming data credits" do
    {_server, connection} =
      start_connection(
        max_in_flight: 1,
        max_in_flight_per_lane: 1,
        response_fun: fn
          %{opcode: 0x000D} ->
            %{
              "accepted" => true,
              "limits" => %{
                "max_inflight_per_connection" => 1,
                "max_inflight_per_lane" => 1
              }
            }

          _request ->
            "OK"
        end
      )

    assert :ok =
             Connection.complete_bootstrap(connection, %{
               "capabilities" => %{
                 "flow_control" => %{
                   "max_inflight_per_connection" => 0,
                   "max_inflight_per_lane" => 0
                 }
               }
             })

    assert {:ok, %{"accepted" => true}} =
             Connection.request(connection, 0x000D, %{"credits" => 1}, 0, 200)

    assert %{max_in_flight: 1, max_in_flight_per_lane: 1} = :sys.get_state(connection)
    assert {:ok, "OK"} = Connection.request(connection, 0x0101, %{"key" => "ready"}, 1, 200)
  end

  test "control requests remain available while the data window is saturated" do
    {_server, connection} =
      start_connection(
        max_in_flight: 1,
        max_in_flight_per_lane: 1,
        response_fun: fn
          %{opcode: 0x000D} -> %{"accepted" => false}
          _request -> :noreply
        end
      )

    data =
      Task.async(fn -> Connection.request(connection, 0x0101, %{"key" => "held"}, 1, 100) end)

    assert_eventually(fn -> :sys.get_state(connection).data_in_flight == 1 end)

    assert {:ok, %{"accepted" => false}} =
             Connection.request(connection, 0x000D, %{"credits" => 1}, 0, 100)

    assert {:error, :connection_backpressure} =
             Connection.request(connection, 0x0101, %{"key" => "blocked"}, 1, 100)

    assert Task.await(data, 200) == {:error, :timeout}
  end

  test "outbound request bodies are rejected before socket send when over budget" do
    {_server, connection} = start_connection(max_request_bytes: 64)

    assert {:error, :request_too_large} =
             Connection.request(
               connection,
               0x0102,
               %{"key" => "large", "value" => String.duplicate("x", 128)},
               1,
               200
             )

    refute_receive {:native_server_request, %{opcode: 0x0102}}, 100
    assert Process.alive?(connection)
  end

  test "connection inspection excludes endpoint credentials and buffered values" do
    secret = "connection-state-secret"

    state = %Connection{
      endpoint: %{password: secret},
      buffer: FrameStream.new() |> FrameStream.append(secret),
      pending: %{1 => %{chunks: [secret]}},
      server_frame_assembler: %ServerFrameAssembler{
        streams: %{{0, 0} => %{chunks: [secret]}}
      }
    }

    refute inspect(state, limit: :infinity, printable_limit: :infinity) =~ secret
  end

  test "timed-out sent requests retain flow-control credit until their late response" do
    {server, connection} =
      start_connection(response_fun: fn _request -> :noreply end, max_in_flight: 1)

    first =
      Task.async(fn ->
        Connection.request(connection, 0x0101, %{"key" => "first"}, 1, 5_000)
      end)

    assert_receive {:native_server_request, first_request}, 1_000

    %{timeout_token: first_timeout_token, timer: first_timer} =
      :sys.get_state(connection).pending[first_request.request_id]

    Process.cancel_timer(first_timer, async: false, info: false)
    send(connection, {:request_timeout, first_request.request_id, first_timeout_token})

    assert Task.await(first, 1_000) == {:error, :timeout}

    assert %{data_in_flight: 1, pending: pending} = :sys.get_state(connection)
    assert pending[first_request.request_id].phase == :discarding

    assert {:error, :connection_backpressure} =
             Connection.request(connection, 0x0101, %{"key" => "blocked"}, 1, 5_000)

    refute_receive {:native_server_request, %{payload: %{"key" => "blocked"}}}, 50

    late_body = <<0::unsigned-16, Codec.encode_value("late")::binary>>
    assert [:ok] = NativeServer.send_raw(server, raw_response_frame(first_request, 0, late_body))

    assert_eventually(fn ->
      state = :sys.get_state(connection)
      state.data_in_flight == 0 and state.pending == %{}
    end)

    second =
      Task.async(fn ->
        Connection.request(connection, 0x0101, %{"key" => "second"}, 1, 5_000)
      end)

    assert_receive {:native_server_request, second_request}, 1_000
    assert first_request.request_id != second_request.request_id

    body = <<0::unsigned-16, Codec.encode_value("current")::binary>>
    assert [:ok] = NativeServer.send_raw(server, raw_response_frame(second_request, 0, body))

    assert Task.await(second, 1_000) == {:ok, "current"}
    assert :sys.get_state(connection).pending == %{}
    assert connection |> :sys.get_state() |> Map.fetch!(:buffer) |> FrameStream.empty?()
  end

  test "cancelling a sent request suppresses delivery without releasing server credit early" do
    {server, connection} =
      start_connection(response_fun: fn _request -> :noreply end, max_in_flight: 1)

    tag = make_ref()

    assert :ok =
             Connection.async_request(connection, self(), tag, 0x0101, %{"key" => "k"}, 1, 1_000)

    assert_eventually(fn -> map_size(:sys.get_state(connection).pending) == 1 end)
    assert_receive {:native_server_request, request}, 1_000

    state = :sys.get_state(connection)
    [{request_id, _request}] = Map.to_list(state.pending)
    assert state.pending_targets == %{{:message, self(), tag} => request_id}

    assert :ok = Connection.cancel(connection, self(), tag)

    assert_eventually(fn ->
      state = :sys.get_state(connection)

      state.data_in_flight == 1 and state.pending_targets == %{} and
        state.pending[request_id].phase == :discarding
    end)

    refute_receive {:ferricstore_connection_response, ^connection, ^tag, _result}, 50

    assert {:error, :connection_backpressure} =
             Connection.request(connection, 0x0101, %{"key" => "blocked"}, 1, 20)

    refute_receive {:native_server_request, %{payload: %{"key" => "blocked"}}}, 50

    body = <<0::unsigned-16, Codec.encode_value("cancelled")::binary>>
    assert [:ok] = NativeServer.send_raw(server, raw_response_frame(request, 0, body))

    assert_eventually(fn ->
      state = :sys.get_state(connection)
      state.data_in_flight == 0 and state.pending == %{}
    end)

    next_tag = make_ref()

    assert :ok =
             Connection.async_request(
               connection,
               self(),
               next_tag,
               0x0101,
               %{"key" => "next"},
               1,
               1_000
             )

    assert_receive {:native_server_request, next_request}, 1_000
    next_body = <<0::unsigned-16, Codec.encode_value("current")::binary>>
    assert [:ok] = NativeServer.send_raw(server, raw_response_frame(next_request, 0, next_body))

    assert_receive {:ferricstore_connection_response, ^connection, ^next_tag, {:ok, "current"}},
                   1_000
  end

  @tag capture_log: true
  test "a sent request without a late response retires the uncertain connection" do
    {_server, connection} =
      start_connection(
        response_fun: fn _request -> :noreply end,
        max_in_flight: 1,
        heartbeat_interval: :infinity
      )

    monitor = Process.monitor(connection)

    assert {:error, :timeout} =
             Connection.request(connection, 0x0101, %{"key" => "missing"}, 1, 30)

    assert_receive {:DOWN, ^monitor, :process, ^connection, :late_response_timeout}, 1_000
  end

  test "draining has a finite deadline even when an in-flight request has no timeout" do
    {_server, connection} =
      start_connection(
        response_fun: fn _request -> :noreply end,
        drain_timeout: 30,
        heartbeat_interval: :infinity
      )

    tag = make_ref()

    assert :ok =
             Connection.async_request(
               connection,
               self(),
               tag,
               0x0101,
               %{"key" => "held"},
               1,
               :infinity
             )

    assert_eventually(fn -> map_size(:sys.get_state(connection).pending) == 1 end)
    monitor = Process.monitor(connection)

    assert :ok = Connection.drain(connection)

    assert_receive {:ferricstore_connection_response, ^connection, ^tag,
                    {:error, :connection_drained}},
                   200

    assert_receive {:DOWN, ^monitor, :process, ^connection, :normal}, 200
  end

  test "mass async cancellation scales near-linearly" do
    cancel_reductions(24)
    small = cancel_reductions(96)
    large = cancel_reductions(192)

    assert large < small * 3
  end

  @tag capture_log: true
  test "partial response buffering is bounded across all in-flight requests" do
    {_server, connection} =
      start_connection(
        max_response_bytes: 64,
        max_response_buffer_bytes: 64,
        response_fun: fn request ->
          {:raw, raw_response_frame(request, 0x20, String.duplicate("x", 40))}
        end
      )

    monitor = Process.monitor(connection)
    first = make_ref()
    second = make_ref()

    Connection.async_request(connection, self(), first, 0x0101, %{"key" => "one"}, 1, 500)
    Connection.async_request(connection, self(), second, 0x0101, %{"key" => "two"}, 1, 500)

    assert_receive {:DOWN, ^monitor, :process, ^connection, :response_buffers_too_large}, 500
  end

  test "data requests carry an absolute server deadline" do
    {_server, connection} = start_connection()

    before_ms = System.system_time(:millisecond)
    assert {:ok, "OK"} = Connection.request(connection, 0x0101, %{"key" => "k"}, 1, 250)
    after_ms = System.system_time(:millisecond)

    assert_receive {:native_server_request,
                    %{opcode: 0x0101, payload: %{"deadline_ms" => deadline_ms}}}

    assert deadline_ms >= before_ms + 200
    assert deadline_ms <= after_ms + 250
  end

  test "caller payloads cannot extend the server deadline past the request timeout" do
    {_server, connection} = start_connection()
    caller_deadline = System.system_time(:millisecond) + 60_000
    before_ms = System.system_time(:millisecond)

    assert {:ok, "OK"} =
             Connection.request(
               connection,
               0x0101,
               %{"key" => "k", "deadline_ms" => caller_deadline},
               1,
               250
             )

    after_ms = System.system_time(:millisecond)

    assert_receive {:native_server_request,
                    %{opcode: 0x0101, payload: %{"deadline_ms" => deadline_ms}}}

    assert deadline_ms < caller_deadline
    assert deadline_ms >= before_ms + 200
    assert deadline_ms <= after_ms + 250
  end

  test "server events are delivered and GOAWAY drains the connection" do
    {server, connection} = start_connection(event_handler: self())

    assert_eventually(fn -> NativeServer.connection_count(server) == 1 end)
    assert [:ok] = NativeServer.send_event(server, %{"kind" => "wake"}, force: true)

    assert_receive {:ferricstore_server_frame, ^connection, 0x0010, %{"kind" => "wake"}}

    monitor = Process.monitor(connection)

    assert [:ok] =
             NativeServer.send_event(server, %{"reason" => "maintenance"}, opcode: 0x000A)

    assert_receive {:ferricstore_server_frame, ^connection, 0x000A, %{"reason" => "maintenance"}}

    assert_receive {:DOWN, ^monitor, :process, ^connection, _reason}, 500
  end

  @tag capture_log: true
  test "connection-level server errors fail correlated requests with their status" do
    {server, connection} =
      start_connection(response_fun: fn _request -> :noreply end, event_handler: self())

    request =
      Task.async(fn ->
        Connection.request(connection, 0x0101, %{"key" => "blocked"}, 1, 5_000)
      end)

    assert_receive {:native_server_request, %{opcode: 0x0101}}, 1_000
    monitor = Process.monitor(connection)
    body = <<1::16, Codec.encode_value("ERR TLS required")::binary>>

    assert [:ok] = NativeServer.send_raw(server, raw_server_frame(0, 0, body))

    assert Task.await(request, 1_000) ==
             {:error, {:server_error, :error, "ERR TLS required"}}

    assert_receive {:DOWN, ^monitor, :process, ^connection,
                    {:server_error, :error, "ERR TLS required"}},
                   1_000

    refute_receive {:ferricstore_server_frame, ^connection, 0, _value}
  end

  @tag capture_log: true
  test "reserved request-id frames reject data lanes and command opcodes" do
    {server, connection} = start_connection(event_handler: self())
    monitor = Process.monitor(connection)
    body = <<0::16, Codec.encode_value("invalid unsolicited response")::binary>>

    assert_eventually(fn -> NativeServer.connection_count(server) == 1 end)
    assert [:ok] = NativeServer.send_raw(server, raw_server_frame(0x0101, 0, body, 1))

    assert_receive {:DOWN, ^monitor, :process, ^connection,
                    {:invalid_server_frame, %{lane_id: 1, opcode: 0x0101}}},
                   1_000

    refute_receive {:ferricstore_server_frame, ^connection, 0x0101, _value}
  end

  test "a slow function event handler does not block connection requests" do
    test_pid = self()

    handler = fn event ->
      send(test_pid, {:connection_handler_started, self(), event})

      receive do
        :release_handler -> :ok
      end
    end

    {server, connection} = start_connection(event_handler: handler)
    assert_eventually(fn -> NativeServer.connection_count(server) == 1 end)
    assert [:ok] = NativeServer.send_event(server, %{"kind" => "slow"}, force: true)

    assert_receive {:connection_handler_started, handler_worker,
                    %{connection: ^connection, opcode: 0x0010, value: %{"kind" => "slow"}}},
                   1_000

    request =
      Task.async(fn -> Connection.request(connection, 0x0101, %{"key" => "ready"}, 1, 500) end)

    try do
      assert Task.yield(request, 200) == {:ok, {:ok, "OK"}}
    after
      send(handler_worker, :release_handler)
      Task.shutdown(request, :brutal_kill)
    end
  end

  test "GOAWAY drains a slow function event handler before the connection stops" do
    test_pid = self()

    handler = fn
      %{opcode: 0x000A} = event ->
        Process.sleep(50)
        send(test_pid, {:connection_goaway_handled, event})

      _event ->
        :ok
    end

    {server, connection} = start_connection(event_handler: handler)
    assert_eventually(fn -> NativeServer.connection_count(server) == 1 end)
    monitor = Process.monitor(connection)

    assert [:ok] =
             NativeServer.send_event(server, %{"reason" => "maintenance"}, opcode: 0x000A)

    assert_receive {:connection_goaway_handled,
                    %{
                      connection: ^connection,
                      opcode: 0x000A,
                      value: %{"reason" => "maintenance"}
                    }},
                   500

    assert_receive {:DOWN, ^monitor, :process, ^connection, _reason}, 500
  end

  test "the native fixture sends events only to server-side subscribers" do
    {:ok, server} = NativeServer.start_link(owner: self())
    endpoint = %{host: "127.0.0.1", native_port: NativeServer.port(server), event_handler: self()}
    {:ok, subscribed} = Connection.start(endpoint)
    {:ok, unsubscribed} = Connection.start(endpoint)

    on_exit(fn ->
      Enum.each([subscribed, unsubscribed], fn conn ->
        if Process.alive?(conn), do: Connection.close(conn)
      end)

      if Process.alive?(server), do: GenServer.stop(server, :normal)
    end)

    assert {:ok, "OK"} =
             Connection.request(
               subscribed,
               0x0011,
               %{"events" => ["FLOW_WAKE"]},
               0,
               200
             )

    event = %{"event" => "FLOW_WAKE", "payload" => %{}, "at_ms" => 1}
    assert [:ok] = NativeServer.send_event(server, event)
    assert_receive {:ferricstore_server_frame, ^subscribed, 0x0010, ^event}, 200
    refute_receive {:ferricstore_server_frame, ^unsubscribed, 0x0010, ^event}, 50
  end

  @tag capture_log: true
  test "chunked server events are bounded before reassembly" do
    {server, connection} =
      start_connection(max_frame_bytes: 64, max_response_bytes: 64, event_handler: self())

    logical_body = <<0::unsigned-16, Codec.encode_value(String.duplicate("x", 80))::binary>>
    <<first::binary-size(44), second::binary>> = logical_body
    monitor = Process.monitor(connection)

    assert_eventually(fn -> NativeServer.connection_count(server) == 1 end)
    assert [:ok] = NativeServer.send_raw(server, raw_server_frame(0x0010, 0x20, first))
    assert [:ok] = NativeServer.send_raw(server, raw_server_frame(0x0010, 0, second))

    assert_receive {:DOWN, ^monitor, :process, ^connection, :server_frame_too_large}, 500
    refute_receive {:ferricstore_server_frame, ^connection, 0x0010, _value}
  end

  @tag capture_log: true
  test "the number of incomplete server streams is bounded globally" do
    {server, connection} =
      start_connection(max_server_chunk_streams: 1, event_handler: self())

    monitor = Process.monitor(connection)
    assert_eventually(fn -> NativeServer.connection_count(server) == 1 end)

    assert [:ok] = NativeServer.send_raw(server, raw_server_frame(0x0010, 0x20, "one"))

    assert_eventually(fn ->
      connection
      |> :sys.get_state()
      |> Map.fetch!(:server_frame_assembler)
      |> ServerFrameAssembler.stream_count() == 1
    end)

    assert [:ok] = NativeServer.send_raw(server, raw_server_frame(0x000A, 0x20, "two"))

    assert_receive {:DOWN, ^monitor, :process, ^connection, :too_many_server_chunk_streams}, 500
  end

  @tag capture_log: true
  test "an incomplete server stream expires" do
    {server, connection} = start_connection(server_chunk_timeout: 30, event_handler: self())
    monitor = Process.monitor(connection)

    assert_eventually(fn -> NativeServer.connection_count(server) == 1 end)
    assert [:ok] = NativeServer.send_raw(server, raw_server_frame(0x0010, 0x20, "partial"))

    assert_receive {:DOWN, ^monitor, :process, ^connection, :server_chunk_timeout}, 500
  end

  test "compressed responses cannot expand beyond the configured response bound" do
    value = String.duplicate("compressible", 2_000)
    logical_body = <<0::unsigned-16, Codec.encode_value(value)::binary>>
    compressed = :zlib.compress(logical_body)

    {_server, connection} =
      start_connection(
        max_response_bytes: 1_024,
        response_fun: fn request ->
          {:raw, raw_response_frame(request, 0x08, compressed)}
        end
      )

    assert {:error, :decompressed_response_too_large} =
             Connection.request(connection, 0x0101, %{"key" => "bomb"}, 1, 1_000)

    assert :sys.get_state(connection).pending == %{}
    assert Process.alive?(connection)
  end

  test "inbound request activity postpones the idle heartbeat" do
    {_server, connection} = start_connection(heartbeat_interval: 100, heartbeat_timeout: 100)

    assert :ok = Connection.complete_bootstrap(connection, %{})
    Process.sleep(60)

    assert {:ok, "OK"} =
             Connection.request(connection, 0x0101, %{"key" => "active"}, 1, 200)

    assert_receive {:native_server_request, %{opcode: 0x0101}}, 100
    refute_receive {:native_server_request, %{opcode: 0x0003}}, 60
    assert_receive {:native_server_request, %{opcode: 0x0003}}, 80
  end

  @tag capture_log: true
  test "heartbeat timeouts classify unrelated pending work as a transport failure" do
    {_server, connection} =
      start_connection(
        response_fun: fn _request -> :noreply end,
        heartbeat_interval: 10,
        heartbeat_timeout: 20
      )

    pending =
      Task.async(fn ->
        Connection.request(connection, 0x0101, %{"key" => "held"}, 1, 1_000)
      end)

    assert_receive {:native_server_request, %{opcode: 0x0101}}, 200
    assert :ok = Connection.complete_bootstrap(connection, %{})

    assert Task.await(pending, 500) ==
             {:error, {:transport_failed, :heartbeat_timeout}}
  end

  @tag capture_log: true
  test "an unencodable heartbeat closes an unusable negotiated connection" do
    {_server, connection} =
      start_connection(heartbeat_interval: 10, heartbeat_timeout: 100)

    monitor = Process.monitor(connection)

    assert :ok =
             Connection.complete_bootstrap(connection, %{
               "capabilities" => %{"limits" => %{"max_frame_bytes" => 1}}
             })

    assert_receive {:DOWN, ^monitor, :process, ^connection,
                    {:heartbeat_failed, :request_too_large}},
                   500
  end

  defp start_connection(opts \\ []) do
    {server_opts, endpoint_opts} = Keyword.split(opts, [:response_fun])
    server_opts = Keyword.put(server_opts, :owner, self())
    {:ok, server} = NativeServer.start_link(server_opts)
    port = NativeServer.port(server)

    endpoint =
      endpoint_opts
      |> Map.new()
      |> Map.merge(%{host: "127.0.0.1", native_port: port, tls: false})

    {:ok, connection} = Connection.start(endpoint)

    on_exit(fn ->
      Connection.close(connection)
      if Process.alive?(server), do: GenServer.stop(server, :normal)
    end)

    {server, connection}
  end

  defp cancel_reductions(count) do
    {_server, connection} =
      start_connection(
        response_fun: fn _request -> :noreply end,
        max_in_flight: count,
        heartbeat_interval: :infinity
      )

    tags = Enum.map(1..count, fn _index -> make_ref() end)

    Enum.each(tags, fn tag ->
      :ok =
        Connection.async_request(
          connection,
          self(),
          tag,
          0x0101,
          %{"key" => "cancel"},
          1,
          5_000
        )
    end)

    assert_eventually(fn ->
      state = :sys.get_state(connection)

      map_size(state.pending) == count and
        Enum.all?(state.pending, fn {_request_id, pending} -> pending.phase == :sent end)
    end)

    assert map_size(:sys.get_state(connection).pending_targets) == count
    {:reductions, before_count} = Process.info(connection, :reductions)

    Enum.each(tags, &Connection.cancel(connection, self(), &1))

    assert_eventually(fn ->
      state = :sys.get_state(connection)

      map_size(state.pending) == count and state.pending_targets == %{} and
        state.data_in_flight == count and
        Enum.all?(state.pending, fn {_request_id, pending} ->
          pending.phase == :discarding
        end)
    end)

    {:reductions, after_count} = Process.info(connection, :reductions)
    after_count - before_count
  end

  defp raw_response_frame(request, flags, body) do
    <<"FSNP", 0x81, flags, request.lane_id::unsigned-32, request.opcode::unsigned-16,
      request.request_id::unsigned-64, byte_size(body)::unsigned-32, body::binary>>
  end

  defp assert_eventually(fun, attempts \\ 50)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      :ok
    else
      Process.sleep(5)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(fun, 0), do: assert(fun.())

  defp raw_server_frame(opcode, flags, body, lane_id \\ 0) do
    <<"FSNP", 0x81, flags, lane_id::unsigned-32, opcode::unsigned-16, 0::unsigned-64,
      byte_size(body)::unsigned-32, body::binary>>
  end
end
