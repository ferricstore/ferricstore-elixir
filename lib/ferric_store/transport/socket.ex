defmodule FerricStore.Transport.Socket do
  @moduledoc false

  alias FerricStore.Transport.TLS

  @default_timeout 5_000
  @default_send_timeout 5_000

  @type transport :: :gen_tcp | :ssl

  @spec connect(map() | keyword()) :: {:ok, transport(), term()} | {:error, term()}
  def connect(config) when is_map(config) or is_list(config) do
    host = config |> option(:host) |> normalize_host()
    tls? = option(config, :tls, false)
    port = connection_port(config, tls?)
    timeout = option(config, :connect_timeout) || option(config, :timeout, @default_timeout)

    if tls? do
      :ssl.start()
      options = Keyword.merge(TLS.options(config), send_options(config))

      case :ssl.connect(host, port, options, timeout) do
        {:ok, socket} -> {:ok, :ssl, socket}
        {:error, reason} -> {:error, reason}
      end
    else
      options =
        Keyword.merge(
          [mode: :binary, active: false, packet: :raw, nodelay: true],
          send_options(config)
        )

      case :gen_tcp.connect(host, port, options, timeout) do
        {:ok, socket} -> {:ok, :gen_tcp, socket}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @spec send(transport(), term(), iodata()) :: :ok | {:error, term()}
  def send(:gen_tcp, socket, data), do: :gen_tcp.send(socket, data)
  def send(:ssl, socket, data), do: :ssl.send(socket, data)

  @spec recv(transport(), term(), non_neg_integer(), timeout()) ::
          {:ok, binary()} | {:error, term()}
  def recv(:gen_tcp, socket, size, timeout), do: :gen_tcp.recv(socket, size, timeout)
  def recv(:ssl, socket, size, timeout), do: :ssl.recv(socket, size, timeout)

  @spec set_active_once(transport(), term()) :: :ok | {:error, term()}
  def set_active_once(:gen_tcp, socket), do: :inet.setopts(socket, active: :once)
  def set_active_once(:ssl, socket), do: :ssl.setopts(socket, active: :once)

  @spec close(transport(), term()) :: :ok
  def close(:gen_tcp, socket), do: :gen_tcp.close(socket)
  def close(:ssl, socket), do: :ssl.close(socket)

  defp connection_port(config, true) do
    option(config, :native_tls_port) || option(config, :port) ||
      required_option(config, :native_port)
  end

  defp connection_port(config, false) do
    option(config, :port) || required_option(config, :native_port)
  end

  defp required_option(config, key) do
    option(config, key) || raise ArgumentError, "socket configuration requires #{inspect(key)}"
  end

  defp option(config, key, default \\ nil)

  defp option(config, key, default) when is_list(config),
    do: Keyword.get(config, key, default)

  defp option(config, key, default) when is_map(config),
    do: Map.get(config, key, Map.get(config, Atom.to_string(key), default))

  defp send_options(config) do
    timeout =
      case option(config, :send_timeout, @default_send_timeout) do
        value when is_integer(value) and value >= 0 -> value
        _missing_or_invalid -> @default_send_timeout
      end

    [send_timeout: timeout, send_timeout_close: true]
  end

  defp normalize_host(host) when is_binary(host), do: String.to_charlist(host)
  defp normalize_host(host) when is_list(host), do: host
end
