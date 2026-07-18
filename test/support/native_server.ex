defmodule FerricStore.Test.NativeServer do
  @moduledoc false

  use GenServer

  alias FerricStore.Protocol.CapabilityContract
  alias FerricStore.SDK.Native.Codec

  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts)

  def port(server), do: GenServer.call(server, :port)
  def connection_count(server), do: GenServer.call(server, :connection_count)

  def startup_payload(overrides \\ %{}) when is_map(overrides) do
    schemas =
      Map.new(CapabilityContract.required_schemas(), fn {command, required} ->
        {command,
         %{
           "required" => required,
           "fields" => Map.fetch!(CapabilityContract.required_schema_fields(), command)
         }}
      end)

    opcodes =
      Enum.map(CapabilityContract.required_opcodes(), fn command ->
        %{"name" => command.name, "opcode" => command.opcode}
      end)

    base = %{
      "protocol" => "ferricstore-native",
      "version" => 1,
      "compression" => "none",
      "auth_required" => false,
      "capabilities" => %{
        "protocol_versions" => [1],
        "limits" => %{"max_response_bytes" => 64 * 1024 * 1024},
        "response_codecs" => %{
          "compact_response_opcodes" => %{
            "flow_claim_jobs_v1" => [0x0203],
            "flow_record_list_v1" => [0x020E, 0x0217, 0x0218, 0x0219, 0x021A, 0x021B, 0x021D],
            "flow_record_v1" => [0x0202],
            "kv_get_v1" => [0x0101],
            "kv_mget_v1" => [0x0104, 0x020C],
            "ok_list_v1" => [0x0102, 0x0105, 0x020F, 0x0210, 0x0212, 0x0213, 0x0214],
            "pipeline_v1" => [0x000E]
          }
        },
        "schemas" => schemas,
        "opcodes" => opcodes
      }
    }

    deep_merge(base, overrides)
  end

  def raw_startup(payload) when is_map(payload), do: {:raw_startup, payload}

  def send_event(server, value, opts \\ []) do
    GenServer.call(server, {:send_event, value, opts})
  end

  def send_raw(server, data) when is_binary(data) do
    GenServer.call(server, {:send_raw, data})
  end

  @impl true
  def init(opts) do
    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, active: false, packet: :raw, reuseaddr: true])

    {:ok, {_address, port}} = :inet.sockname(listener)
    owner = Keyword.get(opts, :owner, self())
    response_fun = Keyword.get(opts, :response_fun, &default_response(&1, port))
    server = self()
    acceptor = spawn(fn -> accept_loop(listener, server, owner, response_fun) end)

    {:ok,
     %{
       listener: listener,
       acceptor: acceptor,
       owner: owner,
       port: port,
       sockets: %{},
       subscriptions: %{},
       handlers: MapSet.new()
     }}
  end

  @impl true
  def handle_call(:port, _from, state), do: {:reply, state.port, state}

  def handle_call(:connection_count, _from, state),
    do: {:reply, map_size(state.sockets), state}

  def handle_call({:send_event, value, opts}, _from, state) do
    opcode = Keyword.get(opts, :opcode, 0x0010)
    lane_id = Keyword.get(opts, :lane_id, 0)
    request_id = Keyword.get(opts, :request_id, 0)
    frame = response_frame(opcode, lane_id, request_id, value, opts)

    force? = Keyword.get(opts, :force, false) or opcode == 0x000A
    event = event_name(value)

    results =
      state.sockets
      |> Enum.filter(fn {handler, _socket} ->
        force? or
          (is_binary(event) and
             MapSet.member?(Map.get(state.subscriptions, handler, MapSet.new()), event))
      end)
      |> Enum.map(fn {_handler, socket} -> :gen_tcp.send(socket, frame) end)

    {:reply, results, state}
  end

  def handle_call({:apply_subscription, handler, opcode, events, response}, _from, state) do
    subscriptions =
      if successful_response?(response) do
        current = Map.get(state.subscriptions, handler, MapSet.new())
        requested = events |> List.wrap() |> Enum.map(&normalize_event/1) |> MapSet.new()

        next =
          case opcode do
            0x0011 -> MapSet.union(current, requested)
            0x0012 -> MapSet.difference(current, requested)
          end

        Map.put(state.subscriptions, handler, next)
      else
        state.subscriptions
      end

    {:reply, :ok, %{state | subscriptions: subscriptions}}
  end

  def handle_call({:send_raw, data}, _from, state) do
    results = Enum.map(state.sockets, fn {_handler, socket} -> :gen_tcp.send(socket, data) end)
    {:reply, results, state}
  end

  @impl true
  def handle_info({:accepted, handler, socket}, state) do
    monitor = Process.monitor(handler)
    send(state.owner, {:native_server_connected, handler})

    {:noreply,
     %{
       state
       | sockets: Map.put(state.sockets, handler, socket),
         subscriptions: Map.put(state.subscriptions, handler, MapSet.new()),
         handlers: MapSet.put(state.handlers, {handler, monitor})
     }}
  end

  def handle_info({:DOWN, monitor, :process, handler, reason}, state) do
    send(state.owner, {:native_server_disconnected, handler, reason})

    handlers =
      Enum.reduce(state.handlers, MapSet.new(), fn
        {^handler, ^monitor}, acc -> acc
        entry, acc -> MapSet.put(acc, entry)
      end)

    {:noreply,
     %{
       state
       | sockets: Map.delete(state.sockets, handler),
         subscriptions: Map.delete(state.subscriptions, handler),
         handlers: handlers
     }}
  end

  def handle_info({:handler_closed, _handler, _reason}, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    :gen_tcp.close(state.listener)
    Process.exit(state.acceptor, :kill)
    Enum.each(state.sockets, fn {_handler, socket} -> :gen_tcp.close(socket) end)
    Enum.each(state.handlers, fn {handler, _monitor} -> Process.exit(handler, :kill) end)
    :ok
  end

  def response_frame(opcode, lane_id, request_id, value, opts \\ []) do
    status = Keyword.get(opts, :status, 0)
    flags = Keyword.get(opts, :flags, 0)
    payload = <<status::unsigned-16, Codec.encode_value(value)::binary>>

    <<"FSNP", 0x81, flags, lane_id::unsigned-32, opcode::unsigned-16, request_id::unsigned-64,
      byte_size(payload)::unsigned-32, payload::binary>>
  end

  def topology_payload(port, opts \\ []) do
    %{
      "route_epoch" => Keyword.get(opts, :route_epoch, 1),
      "shard_count" => 1,
      "ranges" => [
        %{
          "first_slot" => 0,
          "last_slot" => 1023,
          "shard" => 0,
          "lane_id" => 1,
          "endpoint" => %{
            "node" => Keyword.get(opts, :node, "test-node"),
            "host" => Keyword.get(opts, :host, "127.0.0.1"),
            "native_port" => port
          }
        }
      ]
    }
  end

  defp accept_loop(listener, server, owner, response_fun) do
    case :gen_tcp.accept(listener) do
      {:ok, socket} ->
        handler = spawn(fn -> await_socket(server, owner, response_fun) end)
        :ok = :gen_tcp.controlling_process(socket, handler)
        send(handler, {:socket, socket})
        send(server, {:accepted, handler, socket})
        accept_loop(listener, server, owner, response_fun)

      {:error, _reason} ->
        :ok
    end
  end

  defp await_socket(server, owner, response_fun) do
    receive do
      {:socket, socket} -> serve(socket, server, owner, response_fun)
    end
  end

  defp serve(socket, server, owner, response_fun) do
    case recv_request(socket) do
      {:ok, request} ->
        request = Map.put(request, :socket, socket)
        send(owner, {:native_server_request, request})
        response = request |> response_fun.() |> normalize_response(request)
        maybe_apply_subscription(server, request, response)
        respond(socket, request, response)
        serve(socket, server, owner, response_fun)

      {:error, reason} ->
        send(server, {:handler_closed, self(), reason})
        :ok
    end
  rescue
    error ->
      send(server, {:handler_closed, self(), {:exception, error}})
      :ok
  end

  defp recv_request(socket) do
    with {:ok,
          <<"FSNP", _version, flags, lane_id::unsigned-32, opcode::unsigned-16,
            request_id::unsigned-64, body_len::unsigned-32>>} <- :gen_tcp.recv(socket, 24),
         {:ok, body} <- recv_body(socket, body_len) do
      {:ok,
       %{
         flags: flags,
         lane_id: lane_id,
         opcode: opcode,
         request_id: request_id,
         payload: decode_payload(body)
       }}
    end
  end

  defp recv_body(_socket, 0), do: {:ok, <<>>}
  defp recv_body(socket, size), do: :gen_tcp.recv(socket, size)

  defp decode_payload(body) do
    case Codec.decode_value(body) do
      {:ok, value, ""} -> value
      _other -> body
    end
  end

  defp maybe_apply_subscription(server, %{opcode: opcode, payload: payload}, response)
       when opcode in [0x0011, 0x0012] and is_map(payload) do
    GenServer.call(
      server,
      {:apply_subscription, self(), opcode, Map.get(payload, "events", []), response}
    )
  end

  defp maybe_apply_subscription(_server, _request, _response), do: :ok

  defp successful_response?({:reply, _value}), do: true
  defp successful_response?({:reply_after, _delay, _value}), do: true

  defp successful_response?({:reply, _value, opts}),
    do: Keyword.get(opts, :status, 0) == 0

  defp successful_response?({:reply_after, _delay, _value, opts}),
    do: Keyword.get(opts, :status, 0) == 0

  defp successful_response?(:noreply), do: false
  defp successful_response?(:close), do: false
  defp successful_response?({:raw, _frame}), do: false
  defp successful_response?(_value), do: true

  defp event_name(%{"event" => event}), do: normalize_event(event)
  defp event_name(%{event: event}), do: normalize_event(event)
  defp event_name(%{"kind" => event}), do: normalize_event(event)
  defp event_name(%{kind: event}), do: normalize_event(event)
  defp event_name(_value), do: nil

  defp normalize_event(event) when is_atom(event),
    do: event |> Atom.to_string() |> String.upcase()

  defp normalize_event(event) when is_binary(event), do: String.upcase(event)
  defp normalize_event(event), do: event

  defp respond(_socket, _request, :noreply), do: :ok
  defp respond(socket, request, {:reply, value}), do: send_response(socket, request, value, [])

  defp respond(socket, request, {:reply, value, opts}),
    do: send_response(socket, request, value, opts)

  defp respond(socket, request, {:reply_after, delay_ms, value}) do
    spawn(fn ->
      Process.sleep(delay_ms)
      send_response(socket, request, value, [])
    end)

    :ok
  end

  defp respond(socket, request, {:reply_after, delay_ms, value, opts}) do
    spawn(fn ->
      Process.sleep(delay_ms)
      send_response(socket, request, value, opts)
    end)

    :ok
  end

  defp respond(socket, _request, {:raw, frame}), do: :gen_tcp.send(socket, frame)
  defp respond(socket, _request, :close), do: :gen_tcp.close(socket)
  defp respond(socket, request, value), do: send_response(socket, request, value, [])

  defp send_response(socket, request, value, opts) do
    request
    |> then(&response_frame(&1.opcode, &1.lane_id, &1.request_id, value, opts))
    |> then(&:gen_tcp.send(socket, &1))
  end

  defp default_response(%{opcode: 0x0007}, port) do
    topology_payload(port)
  end

  defp default_response(%{opcode: opcode}, _port) when opcode in [0x0001, 0x000C],
    do: startup_payload()

  defp default_response(_request, _port), do: "OK"

  defp normalize_response({:raw_startup, payload}, %{opcode: opcode})
       when opcode in [0x0001, 0x000C],
       do: payload

  defp normalize_response({:reply, value, opts}, %{opcode: opcode})
       when opcode in [0x0001, 0x000C] and is_map(value),
       do: {:reply, startup_payload(value), opts}

  defp normalize_response({:reply_after, delay, value, opts}, %{opcode: opcode})
       when opcode in [0x0001, 0x000C] and is_map(value),
       do: {:reply_after, delay, startup_payload(value), opts}

  defp normalize_response({:reply, value}, %{opcode: opcode})
       when opcode in [0x0001, 0x000C] and is_map(value),
       do: {:reply, startup_payload(value)}

  defp normalize_response({:reply_after, delay, value}, %{opcode: opcode})
       when opcode in [0x0001, 0x000C] and is_map(value),
       do: {:reply_after, delay, startup_payload(value)}

  defp normalize_response(value, %{opcode: opcode})
       when opcode in [0x0001, 0x000C] and is_map(value),
       do: startup_payload(value)

  defp normalize_response("OK", %{opcode: opcode}) when opcode in [0x0001, 0x000C],
    do: startup_payload()

  defp normalize_response(response, _request), do: response

  defp deep_merge(left, right) do
    Map.merge(left, right, fn _key, left_value, right_value ->
      if is_map(left_value) and is_map(right_value),
        do: deep_merge(left_value, right_value),
        else: right_value
    end)
  end
end
