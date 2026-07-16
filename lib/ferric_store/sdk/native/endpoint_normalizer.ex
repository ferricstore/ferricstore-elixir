defmodule FerricStore.SDK.Native.EndpointNormalizer do
  @moduledoc false

  alias FerricStore.SDK.Native.{ConnectionOptions, EndpointIdentity, Topology}

  @endpoint_option_keys ConnectionOptions.keys()
  @endpoint_keys [
                   :node,
                   :host,
                   :native_port,
                   :native_tls_port,
                   :port,
                   :tls
                 ] ++ @endpoint_option_keys
  @endpoint_key_set MapSet.new(@endpoint_keys ++ Enum.map(@endpoint_keys, &Atom.to_string/1))
  @max_endpoint_keys length(@endpoint_keys)

  @spec options(keyword()) :: map()
  def options(opts) when is_list(opts) do
    opts
    |> Keyword.take(@endpoint_option_keys)
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Map.new()
    |> Topology.prepare_endpoint()
  end

  @spec normalize_seeds(list(), boolean(), map()) :: {:ok, [map()]} | {:error, term()}
  def normalize_seeds(seeds, tls, endpoint_options) when is_list(seeds) do
    seeds
    |> Enum.reduce_while({:ok, []}, fn seed, {:ok, normalized} ->
      case normalize_seed(seed, tls, endpoint_options) do
        {:ok, endpoint} -> {:cont, {:ok, [endpoint | normalized]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, normalized} -> {:ok, Enum.reverse(normalized)}
      {:error, reason} -> {:error, reason}
    end
  end

  @spec normalize(term()) :: {:ok, map()} | {:error, {:invalid_endpoint, term()}}
  def normalize({host, port} = endpoint) do
    with {:ok, host} <- EndpointIdentity.normalize_dns_result(host),
         true <- valid_port?(port) do
      {:ok, %{host: host, native_port: port}}
    else
      _invalid -> {:error, {:invalid_endpoint, endpoint}}
    end
  end

  def normalize(endpoint) when is_map(endpoint) and map_size(endpoint) > @max_endpoint_keys,
    do: {:error, {:invalid_endpoint, endpoint}}

  def normalize(endpoint) when is_map(endpoint) do
    with {:ok, normalized} <- normalize_keys(endpoint),
         {:ok, normalized} <- canonicalize_native_port(normalized),
         {:ok, host} <- Map.fetch(normalized, :host),
         {:ok, port} <- Map.fetch(normalized, :native_port),
         {:ok, host} <- EndpointIdentity.normalize_dns_result(host),
         true <- valid_port?(port),
         true <- valid_optional_port?(Map.get(normalized, :native_tls_port)),
         true <- valid_optional_port?(Map.get(normalized, :port)),
         true <- valid_optional_boolean?(Map.get(normalized, :tls)),
         normalized = Map.put(normalized, :host, host),
         :ok <- validate_options(normalized) do
      {:ok, Topology.prepare_endpoint(normalized)}
    else
      _invalid -> {:error, {:invalid_endpoint, endpoint}}
    end
  end

  def normalize(endpoint), do: {:error, {:invalid_endpoint, endpoint}}

  @spec apply_options(map(), map()) :: map()
  def apply_options(endpoint, endpoint_options)
      when is_map(endpoint) and is_map(endpoint_options) do
    Enum.reduce(endpoint_options, endpoint, fn {key, value}, acc ->
      Map.put_new(acc, key, value)
    end)
  end

  defp normalize_seed({host, port}, tls, endpoint_options),
    do: normalize_seed(%{host: host, native_port: port}, tls, endpoint_options)

  defp normalize_seed(seed, tls, endpoint_options) when is_map(seed) do
    with {:ok, seed} <- normalize(seed) do
      endpoint =
        seed
        |> Map.put_new(:node, seed.host)
        |> Map.put_new(:tls, tls)
        |> apply_options(endpoint_options)
        |> Topology.prepare_endpoint()

      {:ok, endpoint}
    end
  end

  defp normalize_seed(seed, _tls, _endpoint_options),
    do: {:error, {:invalid_endpoint, seed}}

  defp normalize_keys(endpoint) do
    if Enum.all?(Map.keys(endpoint), &MapSet.member?(@endpoint_key_set, &1)),
      do: normalize_known_keys(endpoint),
      else: :unknown
  end

  defp normalize_known_keys(endpoint) do
    Enum.reduce_while(@endpoint_keys, {:ok, %{}}, fn key, {:ok, normalized} ->
      string_key = Atom.to_string(key)

      case {Map.fetch(endpoint, key), Map.fetch(endpoint, string_key)} do
        {{:ok, _atom_value}, {:ok, _string_value}} -> {:halt, :ambiguous}
        {{:ok, value}, :error} -> {:cont, {:ok, Map.put(normalized, key, value)}}
        {:error, {:ok, value}} -> {:cont, {:ok, Map.put(normalized, key, value)}}
        {:error, :error} -> {:cont, {:ok, normalized}}
      end
    end)
  end

  defp validate_options(endpoint) do
    case ConnectionOptions.validate(endpoint) do
      :ok -> :ok
      {:error, _option} -> :error
    end
  end

  defp canonicalize_native_port(%{native_port: _port} = endpoint), do: {:ok, endpoint}

  defp canonicalize_native_port(%{tls: true, native_tls_port: port} = endpoint),
    do: {:ok, Map.put(endpoint, :native_port, port)}

  defp canonicalize_native_port(%{port: port} = endpoint),
    do: {:ok, Map.put(endpoint, :native_port, port)}

  defp canonicalize_native_port(_endpoint), do: :error

  defp valid_port?(port), do: is_integer(port) and port in 1..65_535
  defp valid_optional_port?(nil), do: true
  defp valid_optional_port?(port), do: valid_port?(port)
  defp valid_optional_boolean?(nil), do: true
  defp valid_optional_boolean?(value), do: is_boolean(value)
end
