defmodule FerricStore.SDK.Native.Topology.EndpointResolver do
  @moduledoc false

  alias FerricStore.SDK.Native.EndpointIdentity

  @identity_fields [
    "host",
    "native_host",
    "native_port",
    "node",
    "leader_node",
    "owner_node",
    "native_tls_port"
  ]

  @spec resolve(map(), map(), map()) ::
          {:ok, map(), term(), map()} | {:error, :invalid_endpoint}
  def resolve(range, default_endpoint, cache) do
    source = endpoint_source(range)
    cache_key = Map.take(source, @identity_fields)

    case Map.fetch(cache, cache_key) do
      {:ok, {endpoint, endpoint_key}} ->
        {:ok, endpoint, endpoint_key, cache}

      :error ->
        with {:ok, endpoint} <- endpoint_from_map(source, default_endpoint) do
          endpoint_key = EndpointIdentity.key(endpoint)
          {:ok, endpoint, endpoint_key, Map.put(cache, cache_key, {endpoint, endpoint_key})}
        end
    end
  end

  defp endpoint_source(%{"endpoint" => endpoint}) when is_map(endpoint), do: endpoint
  defp endpoint_source(inline_endpoint), do: inline_endpoint

  defp endpoint_from_map(map, default_endpoint) do
    with {:ok, host} <-
           EndpointIdentity.normalize_dns_result(
             map["host"] || map["native_host"] || default_host(default_endpoint)
           ),
         port when is_integer(port) <- map["native_port"] || default_port(default_endpoint),
         true <- valid_port?(port),
         node when is_binary(node) <-
           map["node"] || map["leader_node"] || map["owner_node"] || host,
         tls_port = map["native_tls_port"] || default_tls_port(default_endpoint),
         true <- is_nil(tls_port) or valid_port?(tls_port) do
      endpoint =
        default_endpoint
        |> EndpointIdentity.inherited_options()
        |> Map.merge(%{node: node, host: host, native_port: port})
        |> maybe_put(:native_tls_port, tls_port)

      {:ok, endpoint}
    else
      _invalid -> {:error, :invalid_endpoint}
    end
  end

  defp default_host(%{host: host}) when is_binary(host), do: host
  defp default_host(%{"host" => host}) when is_binary(host), do: host
  defp default_host(_default_endpoint), do: nil

  defp default_port(%{native_port: port}) when is_integer(port), do: port
  defp default_port(%{"native_port" => port}) when is_integer(port), do: port
  defp default_port(_default_endpoint), do: nil

  defp default_tls_port(%{native_tls_port: port}) when is_integer(port), do: port
  defp default_tls_port(%{"native_tls_port" => port}) when is_integer(port), do: port
  defp default_tls_port(_default_endpoint), do: nil

  defp valid_port?(port), do: is_integer(port) and port >= 1 and port <= 65_535

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
