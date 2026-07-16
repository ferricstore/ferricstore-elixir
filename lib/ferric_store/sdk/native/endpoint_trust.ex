defmodule FerricStore.SDK.Native.EndpointTrust do
  @moduledoc false

  alias FerricStore.SDK.Native.EndpointIdentity

  defstruct seed_endpoints: MapSet.new(), hosts: MapSet.new()

  @type t :: %__MODULE__{
          seed_endpoints: MapSet.t(term()),
          hosts: MapSet.t(binary())
        }

  @spec new([map()], keyword()) :: t()
  def new(seeds, opts) when is_list(seeds) and is_list(opts) do
    configured_hosts = Keyword.get(opts, :trusted_hosts, [])

    %__MODULE__{
      seed_endpoints: seeds |> Enum.map(&EndpointIdentity.key/1) |> MapSet.new(),
      hosts: host_set(Enum.map(seeds, &Map.get(&1, :host)) ++ configured_hosts)
    }
  end

  @spec from_hosts([binary() | atom()]) :: t()
  def from_hosts(hosts) when is_list(hosts), do: %__MODULE__{hosts: host_set(hosts)}

  @spec seed_endpoint?(t(), map()) :: boolean()
  def seed_endpoint?(%__MODULE__{seed_endpoints: endpoints}, endpoint) when is_map(endpoint),
    do: MapSet.member?(endpoints, EndpointIdentity.key(endpoint))

  @spec host?(t(), map()) :: boolean()
  def host?(%__MODULE__{hosts: hosts}, endpoint) when is_map(endpoint) do
    case normalize_host(Map.get(endpoint, :host, Map.get(endpoint, "host"))) do
      nil -> false
      host -> MapSet.member?(hosts, host)
    end
  end

  defp host_set(hosts) do
    hosts
    |> Enum.reduce(MapSet.new(), fn host, normalized ->
      case normalize_host(host) do
        nil -> normalized
        host -> MapSet.put(normalized, host)
      end
    end)
  end

  defp normalize_host(host) when is_binary(host) do
    case EndpointIdentity.normalize_dns_result(host) do
      {:ok, normalized} -> normalized
      {:error, :invalid_endpoint_name} -> nil
    end
  end

  defp normalize_host(host) when is_atom(host) and host not in [nil, true, false],
    do: host |> Atom.to_string() |> normalize_host()

  defp normalize_host(_host), do: nil
end
