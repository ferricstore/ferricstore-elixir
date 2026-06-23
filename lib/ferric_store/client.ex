defmodule FerricStore.Client do
  @moduledoc """
  Synchronous native-protocol client.

  A client process owns one `ferric://` socket. Calls are serialized through the
  process, which keeps the first implementation safe and predictable. Throughput
  pooling/multiplexing can be layered above this API without changing callers.
  """

  use GenServer

  alias FerricStore.Error
  alias FerricStore.Protocol

  @default_url "ferric://127.0.0.1:6388"
  @default_timeout 5_000

  defstruct [:socket, :transport, :host, :port, :tls, :request_id, :timeout]

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

  def native(client, opcode, payload, opts \\ []) do
    GenServer.call(client, {:native, opcode, payload, opts}, timeout(opts))
  end

  def close(client) do
    GenServer.call(client, :close)
  catch
    :exit, _ -> :ok
  end

  @impl true
  def init(opts) do
    opts = normalize_opts(opts)

    with {:ok, state} <- connect_socket(opts),
         {:ok, _value, state} <-
           request(state, Protocol.opcode(:startup), startup_payload(opts), 0),
         {:ok, state} <- maybe_auth(state, opts) do
      {:ok, state}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:command, command, args, _opts}, _from, state) do
    payload = Protocol.command_payload(command, args)

    case request(state, Protocol.opcode(:command_exec), payload, 1) do
      {:ok, value, state} -> {:reply, value, state}
      {:error, error, state} -> {:reply, {:error, error}, state}
    end
  end

  def handle_call({:pipeline, commands, _opts}, _from, state) do
    payload = Protocol.pipeline_payload(commands)

    case request(state, Protocol.opcode(:pipeline), payload, 1) do
      {:ok, value, state} -> {:reply, value, state}
      {:error, error, state} -> {:reply, {:error, error}, state}
    end
  end

  def handle_call({:native, opcode, payload, opts}, _from, state) do
    lane_id = Keyword.get(opts, :lane_id, 1)

    case request(state, opcode, payload, lane_id) do
      {:ok, value, state} -> {:reply, value, state}
      {:error, error, state} -> {:reply, {:error, error}, state}
    end
  end

  def handle_call(:close, _from, state) do
    close_socket(state)
    {:stop, :normal, :ok, state}
  end

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

  defp receive_response(state, expected_request_id, expected_opcode) do
    with {:ok, header_binary} <- recv_exact(state, Protocol.header_size()),
         {:ok, header} <- Protocol.decode_response_header(header_binary),
         {:ok, body} <- recv_response_body(state, header) do
      if header.request_id == 0 do
        receive_response(state, expected_request_id, expected_opcode)
      else
        with :ok <- validate_response_header(header, expected_request_id, expected_opcode) do
          case Protocol.decode_response_body(header.flags, header.opcode, body) do
            {:ok, value} ->
              {:ok, value}

            {:error, {status, value}} ->
              {:error, %Error{message: error_message(status, value), status: status, raw: value}}

            {:error, reason} ->
              {:error, reason}
          end
        end
      end
    else
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
