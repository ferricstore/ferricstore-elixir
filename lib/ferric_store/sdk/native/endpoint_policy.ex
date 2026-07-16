defmodule FerricStore.SDK.Native.EndpointPolicy do
  @moduledoc false

  alias FerricStore.SDK.Native.EndpointNormalizer
  alias FerricStore.SDK.Native.EndpointTrust
  alias FerricStore.SDK.Native.EndpointValidator

  @spec compile(term()) :: term()
  def compile({:allow_hosts, hosts}) when is_list(hosts),
    do: {:allow_hosts, EndpointTrust.from_hosts(hosts)}

  def compile(policy), do: policy

  @spec put_server_name(map(), term()) :: map()
  def put_server_name(endpoint, nil), do: endpoint
  def put_server_name(endpoint, server_name), do: Map.put_new(endpoint, :server_name, server_name)

  @spec options(keyword()) :: map()
  defdelegate options(opts), to: EndpointNormalizer

  @spec normalize_seeds(list(), boolean(), map()) :: {:ok, [map()]} | {:error, term()}
  defdelegate normalize_seeds(seeds, tls, endpoint_options), to: EndpointNormalizer

  @spec normalize(term()) :: {:ok, map()} | {:error, {:invalid_endpoint, term()}}
  defdelegate normalize(endpoint), to: EndpointNormalizer

  @spec apply_options(map(), map()) :: map()
  defdelegate apply_options(endpoint, endpoint_options), to: EndpointNormalizer

  @spec validate(term(), EndpointTrust.t(), nil | (map() -> term()), map(), timeout()) ::
          :ok | {:error, term()}
  def validate(policy, endpoint_trust, validator, endpoint, timeout) do
    with :ok <- validate_policy(policy, endpoint_trust, endpoint) do
      EndpointValidator.validate(validator, endpoint, timeout)
    end
  end

  @spec validate_policy(term(), EndpointTrust.t(), map()) :: :ok | {:error, term()}
  def validate_policy(:any, %EndpointTrust{}, _endpoint), do: :ok

  def validate_policy(:none, %EndpointTrust{} = trust, endpoint) do
    if EndpointTrust.seed_endpoint?(trust, endpoint), do: :ok, else: {:error, :unsafe_endpoint}
  end

  def validate_policy(:seed_hosts, %EndpointTrust{} = trust, endpoint) do
    if EndpointTrust.host?(trust, endpoint), do: :ok, else: {:error, :unsafe_endpoint}
  end

  def validate_policy(
        {:allow_hosts, %EndpointTrust{} = allowed_hosts},
        %EndpointTrust{},
        endpoint
      ) do
    if EndpointTrust.host?(allowed_hosts, endpoint),
      do: :ok,
      else: {:error, :unsafe_endpoint}
  end

  def validate_policy(other, _endpoint_trust, _endpoint),
    do: {:error, {:invalid_endpoint_policy, other}}
end
