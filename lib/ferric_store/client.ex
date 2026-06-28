defmodule FerricStore.Client do
  @moduledoc """
  Native-protocol client.

  A client process owns one `ferric://` socket. After startup/authentication the
  default mode is request-id multiplexing: callers can issue concurrent
  `GenServer.call/3` requests, the client sends frames immediately, and replies
  are matched by native protocol request id.
  """

  use GenServer

  alias FerricStore.Error
  alias FerricStore.Protocol

  @default_url "ferric://127.0.0.1:6388"
  @default_timeout 5_000

  defstruct [
    :socket,
    :transport,
    :host,
    :port,
    :tls,
    :request_id,
    :timeout,
    multiplex: true,
    pending: %{},
    buffer: <<>>,
    chunks: %{}
  ]

  @type t :: GenServer.server()

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts)
  end

  def connect!(opts \\ []) do
    case start_link(opts) do
      {:ok, pid} -> pid
      {:error, reason} -> raise Error, message: "connect failed: #{inspect(reason)}", raw: reason
    end
  end

  def command(client, command, args \\ [], opts \\ []) do
    GenServer.call(client, {:command, command, List.wrap(args), opts}, timeout(opts))
  end

  def pipeline(client, commands, opts \\ []) when is_list(commands) do
    GenServer.call(client, {:pipeline, commands, opts}, timeout(opts))
  end

  def async_pipeline(client, commands, opts \\ []) when is_list(commands) do
    ref = make_ref()
    GenServer.cast(client, {:async_pipeline, self(), ref, commands, opts})
    ref
  end

  def native(client, opcode, payload, opts \\ []) do
    GenServer.call(client, {:native, opcode, payload, opts}, timeout(opts))
  end

  def async_native(client, opcode, payload, opts \\ []) do
    ref = make_ref()
    GenServer.cast(client, {:async_native, self(), ref, opcode, payload, opts})
    ref
  end

  def await(ref, timeout \\ @default_timeout) when is_reference(ref) do
    receive do
      {__MODULE__, ^ref, value} -> value
    after
      timeout -> {:error, %Error{message: "FerricStore async request timed out", raw: :timeout}}
    end
  end

  def yield(ref, timeout \\ 0) when is_reference(ref) do
    receive do
      {__MODULE__, ^ref, value} -> {:ok, value}
    after
      timeout -> nil
    end
  end

  def close(client) do
    GenServer.call(client, :close)
  catch
    :exit, _ -> :ok
  end

  @impl true
  def init(opts) do
    opts = normalize_opts(opts)

    multiplex = Keyword.get(opts, :multiplex, true)

    with {:ok, state} <- connect_socket(opts),
         {:ok, _value, state} <-
           request(state, Protocol.opcode(:startup), startup_payload(opts), 0),
         {:ok, state} <- maybe_auth(state, opts) do
      state
      |> Map.put(:multiplex, multiplex)
      |> activate_socket(multiplex)
    else
      {:error, %Error{} = error, _state} -> {:stop, error}
      {:error, reason, _state} -> {:stop, reason}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:command, command, args, opts}, from, state) do
    payload = Protocol.command_payload(command, args, opts)
    dispatch_call_request(state, from, Protocol.opcode(:command_exec), payload, 1)
  end

  def handle_call({:pipeline, commands, opts}, from, state) do
    payload = Protocol.pipeline_payload(commands, opts)
    dispatch_call_request(state, from, Protocol.opcode(:pipeline), payload, 1)
  end

  def handle_call({:native, opcode, payload, opts}, from, state) do
    lane_id = Keyword.get(opts, :lane_id, 1)
    dispatch_call_request(state, from, opcode, payload, lane_id)
  end

  def handle_call(:close, _from, state) do
    close_socket(state)
    {:stop, :normal, :ok, state}
  end

  @impl true
  def handle_cast({:async_pipeline, caller, ref, commands, opts}, state) do
    payload = Protocol.pipeline_payload(commands, opts)
    dispatch_async_request(state, caller, ref, Protocol.opcode(:pipeline), payload, 1)
  end

  def handle_cast({:async_native, caller, ref, opcode, payload, opts}, state) do
    lane_id = Keyword.get(opts, :lane_id, 1)
    dispatch_async_request(state, caller, ref, opcode, payload, lane_id)
  end

  defp dispatch_call_request(%{multiplex: true} = state, from, opcode, payload, lane_id) do
    request_id = next_request_id(state.request_id)
    frame = Protocol.encode_request(opcode, request_id, payload, lane_id: lane_id)

    case send_data(state, frame) do
      :ok ->
        pending = Map.put(state.pending, request_id, {:call, from, opcode})
        {:noreply, %{state | request_id: request_id, pending: pending}}

      {:error, reason} ->
        {:reply, {:error, to_error(reason)}, state}
    end
  end

  defp dispatch_call_request(state, _from, opcode, payload, lane_id) do
    case request(state, opcode, payload, lane_id) do
      {:ok, value, state} -> {:reply, value, state}
      {:error, error, state} -> {:reply, {:error, error}, state}
    end
  end

  defp dispatch_async_request(%{multiplex: true} = state, caller, ref, opcode, payload, lane_id) do
    request_id = next_request_id(state.request_id)
    frame = Protocol.encode_request(opcode, request_id, payload, lane_id: lane_id)

    case send_data(state, frame) do
      :ok ->
        pending = Map.put(state.pending, request_id, {:message, caller, ref, opcode})
        {:noreply, %{state | request_id: request_id, pending: pending}}

      {:error, reason} ->
        send(caller, {__MODULE__, ref, {:error, to_error(reason)}})
        {:noreply, state}
    end
  end

  defp dispatch_async_request(state, caller, ref, opcode, payload, lane_id) do
    value =
      case request(state, opcode, payload, lane_id) do
        {:ok, value, _state} -> value
        {:error, error, _state} -> {:error, error}
      end

    send(caller, {__MODULE__, ref, value})
    {:noreply, state}
  end

  @impl true
  def handle_info({:tcp, socket, data}, %{transport: :gen_tcp, socket: socket} = state),
    do: handle_socket_data(data, state)

  def handle_info({:ssl, socket, data}, %{transport: :ssl, socket: socket} = state),
    do: handle_socket_data(data, state)

  def handle_info({:tcp_closed, socket}, %{transport: :gen_tcp, socket: socket} = state),
    do: handle_socket_down(:closed, state)

  def handle_info({:ssl_closed, socket}, %{transport: :ssl, socket: socket} = state),
    do: handle_socket_down(:closed, state)

  def handle_info({:tcp_error, socket, reason}, %{transport: :gen_tcp, socket: socket} = state),
    do: handle_socket_down(reason, state)

  def handle_info({:ssl_error, socket, reason}, %{transport: :ssl, socket: socket} = state),
    do: handle_socket_down(reason, state)

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state), do: close_socket(state)

  defp normalize_opts(opts) when is_binary(opts), do: normalize_opts(url: opts)

  defp normalize_opts(opts) when is_list(opts) do
    url = Keyword.get(opts, :url, @default_url)
    uri = URI.parse(url)
    scheme = uri.scheme || "ferric"

    unless scheme in ["ferric", "ferrics"] do
      raise ArgumentError, "unsupported FerricStore URL scheme: #{inspect(scheme)}"
    end

    {username, password} = parse_userinfo(uri.userinfo)

    opts
    |> Keyword.put_new(:host, uri.host || "127.0.0.1")
    |> Keyword.put_new(:port, uri.port || default_port(scheme))
    |> Keyword.put_new(:tls, scheme == "ferrics")
    |> Keyword.put_new(:timeout, @default_timeout)
    |> Keyword.put_new(:username, username)
    |> Keyword.put_new(:password, password)
  end

  defp connect_socket(opts) do
    host = Keyword.fetch!(opts, :host)
    port = Keyword.fetch!(opts, :port)
    timeout = Keyword.fetch!(opts, :timeout)
    tls = Keyword.fetch!(opts, :tls)

    if tls do
      :ssl.start()

      case :ssl.connect(String.to_charlist(host), port, ssl_opts(opts), timeout) do
        {:ok, socket} ->
          {:ok,
           %__MODULE__{
             socket: socket,
             transport: :ssl,
             host: host,
             port: port,
             tls: true,
             request_id: 0,
             timeout: timeout
           }}

        {:error, reason} ->
          {:error, reason}
      end
    else
      socket_opts = [:binary, active: false, packet: :raw, nodelay: true]

      case :gen_tcp.connect(String.to_charlist(host), port, socket_opts, timeout) do
        {:ok, socket} ->
          {:ok,
           %__MODULE__{
             socket: socket,
             transport: :gen_tcp,
             host: host,
             port: port,
             tls: false,
             request_id: 0,
             timeout: timeout
           }}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp request(state, opcode, payload, lane_id) do
    request_id = next_request_id(state.request_id)
    frame = Protocol.encode_request(opcode, request_id, payload, lane_id: lane_id)

    with :ok <- send_data(state, frame),
         {:ok, value} <- receive_response(state, request_id, opcode) do
      {:ok, value, %{state | request_id: request_id}}
    else
      {:error, %Error{} = error} -> {:error, error, state}
      {:error, reason} -> {:error, %Error{message: inspect(reason), raw: reason}, state}
    end
  end

  defp handle_socket_data(data, state) do
    state = %{state | buffer: state.buffer <> data}

    case parse_frames(state) do
      {:ok, state} ->
        case set_active_once(state) do
          :ok -> {:noreply, state}
          {:error, reason} -> handle_socket_down(reason, state)
        end

      {:error, reason, state} ->
        handle_socket_down(reason, state)
    end
  end

  defp parse_frames(%{buffer: buffer} = state) do
    case take_frame(buffer) do
      :incomplete ->
        {:ok, state}

      {:ok, header, body, rest} ->
        state
        |> Map.put(:buffer, rest)
        |> process_frame(header, body)
        |> parse_frames()

      {:error, reason} ->
        {:error, reason, state}
    end
  end

  defp take_frame(buffer) do
    header_size = Protocol.header_size()

    case buffer do
      <<header_binary::binary-size(header_size), rest::binary>> ->
        take_frame_body(header_binary, rest)

      _short ->
        :incomplete
    end
  end

  defp take_frame_body(header_binary, rest) do
    case Protocol.decode_response_header(header_binary) do
      {:ok, header} -> take_frame_body(header, rest, header.body_length)
      {:error, reason} -> {:error, reason}
    end
  end

  defp take_frame_body(_header, rest, body_length) when byte_size(rest) < body_length,
    do: :incomplete

  defp take_frame_body(header, rest, body_length) do
    <<body::binary-size(body_length), remaining::binary>> = rest
    {:ok, header, body, remaining}
  end

  defp process_frame(%{chunks: chunks} = state, %{request_id: 0}, _body),
    do: %{state | chunks: chunks}

  defp process_frame(state, header, body) do
    key = {header.request_id, header.opcode, header.lane_id}

    if Bitwise.band(header.flags, Protocol.flag_more_chunks()) != 0 do
      %{state | chunks: Map.update(state.chunks, key, [body], &[body | &1])}
    else
      {previous_chunks, chunks} = Map.pop(state.chunks, key)

      body =
        case previous_chunks do
          nil -> body
          values -> values |> then(&[body | &1]) |> Enum.reverse() |> IO.iodata_to_binary()
        end

      state
      |> Map.put(:chunks, chunks)
      |> complete_response(header, body)
    end
  end

  defp complete_response(state, header, body) do
    case Map.pop(state.pending, header.request_id) do
      {nil, pending} ->
        %{state | pending: pending}

      {{:call, from, expected_opcode}, pending} ->
        GenServer.reply(from, decode_response_reply(header, expected_opcode, body))
        %{state | pending: pending}

      {{:message, caller, ref, expected_opcode}, pending} ->
        send(caller, {__MODULE__, ref, decode_response_reply(header, expected_opcode, body)})
        %{state | pending: pending}
    end
  end

  defp decode_response_reply(header, expected_opcode, body) do
    case validate_response_header(header, header.request_id, expected_opcode) do
      :ok ->
        case Protocol.decode_response_body(header.flags, header.opcode, body) do
          {:ok, value} ->
            value

          {:error, {status, value}} ->
            {:error, %Error{message: error_message(status, value), status: status, raw: value}}

          {:error, %Error{} = error} ->
            {:error, error}

          {:error, reason} ->
            {:error, to_error(reason)}
        end

      {:error, %Error{} = error} ->
        {:error, error}
    end
  end

  defp handle_socket_down(reason, state) do
    state
    |> fail_pending(reason)
    |> close_socket()

    {:stop, reason, %{state | pending: %{}, chunks: %{}, buffer: <<>>}}
  end

  defp fail_pending(state, reason) do
    error = to_error(reason)

    Enum.each(state.pending, fn
      {_request_id, {:call, from, _opcode}} ->
        GenServer.reply(from, {:error, error})

      {_request_id, {:message, caller, ref, _opcode}} ->
        send(caller, {__MODULE__, ref, {:error, error}})
    end)

    state
  end

  defp to_error(%Error{} = error), do: error
  defp to_error(reason), do: %Error{message: inspect(reason), raw: reason}

  defp receive_response(state, expected_request_id, expected_opcode) do
    with {:ok, header_binary} <- recv_exact(state, Protocol.header_size()),
         {:ok, header} <- Protocol.decode_response_header(header_binary),
         {:ok, body} <- recv_response_body(state, header) do
      handle_response_frame(state, header, body, expected_request_id, expected_opcode)
    else
      {:error, {status, value}} ->
        {:error, %Error{message: error_message(status, value), status: status, raw: value}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp handle_response_frame(
         state,
         %{request_id: 0},
         _body,
         expected_request_id,
         expected_opcode
       ),
       do: receive_response(state, expected_request_id, expected_opcode)

  defp handle_response_frame(_state, header, body, expected_request_id, expected_opcode) do
    with :ok <- validate_response_header(header, expected_request_id, expected_opcode) do
      decode_response_result(header, body)
    end
  end

  defp decode_response_result(header, body) do
    case Protocol.decode_response_body(header.flags, header.opcode, body) do
      {:ok, value} ->
        {:ok, value}

      {:error, {status, value}} ->
        {:error, %Error{message: error_message(status, value), status: status, raw: value}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp recv_response_body(state, header) do
    with {:ok, first_body} <- recv_exact(state, header.body_length) do
      if Bitwise.band(header.flags, Protocol.flag_more_chunks()) == 0 do
        {:ok, first_body}
      else
        recv_chunks(state, header, [first_body])
      end
    end
  end

  defp recv_chunks(state, previous_header, chunks) do
    with {:ok, header_binary} <- recv_exact(state, Protocol.header_size()),
         {:ok, header} <- Protocol.decode_response_header(header_binary),
         :ok <- validate_chunk_header(header, previous_header),
         {:ok, body} <- recv_exact(state, header.body_length) do
      chunks = [body | chunks]

      if Bitwise.band(header.flags, Protocol.flag_more_chunks()) == 0 do
        {:ok, chunks |> Enum.reverse() |> IO.iodata_to_binary()}
      else
        recv_chunks(state, header, chunks)
      end
    end
  end

  defp send_data(%{transport: :gen_tcp, socket: socket}, data), do: :gen_tcp.send(socket, data)
  defp send_data(%{transport: :ssl, socket: socket}, data), do: :ssl.send(socket, data)

  defp recv_exact(_state, 0), do: {:ok, <<>>}
  defp recv_exact(state, size), do: recv_exact(state, size, [])
  defp recv_exact(_state, 0, acc), do: {:ok, acc |> Enum.reverse() |> IO.iodata_to_binary()}

  defp recv_exact(state, size, acc) do
    case recv(state, size) do
      {:ok, data} when byte_size(data) == size -> recv_exact(state, 0, [data | acc])
      {:ok, data} -> recv_exact(state, size - byte_size(data), [data | acc])
      {:error, reason} -> {:error, reason}
    end
  end

  defp recv(%{transport: :gen_tcp, socket: socket, timeout: timeout}, size),
    do: :gen_tcp.recv(socket, size, timeout)

  defp recv(%{transport: :ssl, socket: socket, timeout: timeout}, size),
    do: :ssl.recv(socket, size, timeout)

  defp set_active_once(%{transport: :gen_tcp, socket: socket}),
    do: :inet.setopts(socket, active: :once)

  defp set_active_once(%{transport: :ssl, socket: socket}),
    do: :ssl.setopts(socket, active: :once)

  defp validate_response_header(%{request_id: request_id, opcode: opcode}, request_id, opcode),
    do: :ok

  defp validate_response_header(header, request_id, opcode) do
    {:error,
     %Error{
       message:
         "protocol response mismatch: expected request #{request_id}/opcode #{opcode}, got #{header.request_id}/#{header.opcode}",
       raw: header
     }}
  end

  defp validate_chunk_header(header, previous) do
    if header.request_id == previous.request_id and header.opcode == previous.opcode and
         header.lane_id == previous.lane_id do
      :ok
    else
      {:error, %Error{message: "invalid protocol chunk continuation", raw: header}}
    end
  end

  defp maybe_auth(state, opts) do
    password = Keyword.get(opts, :password)

    if password in [nil, ""] do
      {:ok, state}
    else
      payload = %{"username" => Keyword.get(opts, :username), "password" => password}

      case request(state, Protocol.opcode(:auth), payload, 0) do
        {:ok, _value, state} -> {:ok, state}
        {:error, error, _state} -> {:error, error}
      end
    end
  end

  defp activate_socket(state, true) do
    case set_active_once(state) do
      :ok -> {:ok, state}
      {:error, reason} -> {:stop, reason}
    end
  end

  defp activate_socket(state, false), do: {:ok, state}

  defp startup_payload(opts) do
    payload = %{"compression" => "none", "compact_flow_responses" => true}

    case Keyword.get(opts, :client_name) do
      nil -> payload
      name -> Map.merge(payload, %{"client_name" => name, "driver_name" => name})
    end
  end

  defp next_request_id(0xFFFF_FFFF_FFFF_FFFF), do: 1
  defp next_request_id(value), do: value + 1

  defp timeout(opts), do: Keyword.get(opts, :timeout, @default_timeout)

  defp parse_userinfo(nil), do: {nil, nil}

  defp parse_userinfo(userinfo) do
    case String.split(userinfo, ":", parts: 2) do
      [username, password] -> {URI.decode(username), URI.decode(password)}
      [username] -> {URI.decode(username), nil}
    end
  end

  defp default_port("ferric"), do: 6388
  defp default_port("ferrics"), do: 6389

  defp ssl_opts(opts) do
    base = [:binary, active: false, packet: :raw]

    if Keyword.get(opts, :verify, true) do
      [{:verify, :verify_peer} | base]
    else
      [{:verify, :verify_none} | base]
    end
  end

  defp close_socket(%{transport: :gen_tcp, socket: socket}) when not is_nil(socket),
    do: :gen_tcp.close(socket)

  defp close_socket(%{transport: :ssl, socket: socket}) when not is_nil(socket),
    do: :ssl.close(socket)

  defp close_socket(_state), do: :ok

  defp error_message(status, value) when is_binary(value),
    do: "FerricStore error #{status}: #{value}"

  defp error_message(status, value), do: "FerricStore error #{status}: #{inspect(value)}"
end
