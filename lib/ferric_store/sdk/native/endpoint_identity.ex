defmodule FerricStore.SDK.Native.EndpointIdentity do
  @moduledoc false

  alias FerricStore.SDK.Native.{ConnectionOptions, EndpointName}
  alias FerricStore.Transport.CACerts

  @inherited_options [:tls | ConnectionOptions.keys()]

  @spec prepare(map()) :: map()
  def prepare(endpoint) when is_map(endpoint) do
    cond do
      Map.has_key?(endpoint, :cacerts) ->
        Map.update!(endpoint, :cacerts, &prepare_cacerts/1)

      Map.has_key?(endpoint, "cacerts") ->
        endpoint
        |> Map.delete("cacerts")
        |> Map.put(:cacerts, prepare_cacerts(endpoint["cacerts"]))

      true ->
        endpoint
    end
  end

  @spec key(map()) :: tuple()
  def key(endpoint) when is_map(endpoint) do
    host = endpoint |> option(:host) |> normalize_dns()
    tls? = option(endpoint, :tls, false)
    port = effective_port(endpoint, tls?)
    connection_policy = ConnectionOptions.identity(endpoint)

    if tls? do
      {:ssl, host, port, tls_profile(endpoint, host), connection_policy}
    else
      {:gen_tcp, host, port, connection_policy}
    end
  end

  @spec inherited_options(map()) :: map()
  def inherited_options(endpoint) when is_map(endpoint) do
    Enum.reduce(@inherited_options, %{}, fn key, options ->
      case option(endpoint, key) do
        nil -> options
        value -> Map.put(options, key, value)
      end
    end)
  end

  def inherited_options(_endpoint), do: %{}

  @spec normalize_dns(binary() | charlist()) :: binary()
  def normalize_dns(value), do: EndpointName.normalize!(value)

  @spec normalize_dns_result(term()) :: {:ok, binary()} | {:error, :invalid_endpoint_name}
  def normalize_dns_result(value), do: EndpointName.normalize(value)

  defp effective_port(endpoint, true),
    do:
      option(endpoint, :native_tls_port) || option(endpoint, :port) ||
        option(endpoint, :native_port)

  defp effective_port(endpoint, false),
    do: option(endpoint, :port) || option(endpoint, :native_port)

  defp tls_profile(endpoint, host) do
    verify? =
      option(endpoint, :verify, true) != false and option(endpoint, :tls_verify, true) != false

    if verify? do
      server_name = normalize_dns(option(endpoint, :server_name) || host)
      {:verify_peer, server_name, ca_profile(endpoint)}
    else
      :verify_none
    end
  end

  defp ca_profile(endpoint) do
    case {option(endpoint, :cacertfile), option(endpoint, :cacerts)} do
      {cacertfile, _cacerts} when not is_nil(cacertfile) and cacertfile != false ->
        {:cacertfile, normalize_path(cacertfile)}

      {_cacertfile, %CACerts{} = cacerts} ->
        {:cacerts, CACerts.fingerprint(cacerts)}

      {_cacertfile, cacerts} when not is_nil(cacerts) and cacerts != false ->
        {:cacerts, cacerts |> CACerts.prepare() |> CACerts.fingerprint()}

      _no_custom_ca ->
        :system
    end
  end

  defp option(endpoint, key, default \\ nil),
    do: Map.get(endpoint, key, Map.get(endpoint, Atom.to_string(key), default))

  defp normalize_path(value) when is_binary(value), do: value
  defp normalize_path(value) when is_list(value), do: List.to_string(value)

  defp prepare_cacerts(nil), do: nil
  defp prepare_cacerts(%CACerts{} = prepared), do: prepared
  defp prepare_cacerts(certificates) when is_list(certificates), do: CACerts.prepare(certificates)
  defp prepare_cacerts(other), do: other
end
