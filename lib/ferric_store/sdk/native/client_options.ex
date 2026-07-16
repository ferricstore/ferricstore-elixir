defmodule FerricStore.SDK.Native.ClientOptions do
  @moduledoc false

  alias FerricStore.{OptionList, RequestLimits, Timeout, URL}

  alias FerricStore.SDK.Native.{
    ClientCredentialOptions,
    ClientSeedOptions,
    ConnectionOptions,
    EndpointHostList
  }

  @positive_integer_keys [
    :max_pending_requests,
    :max_connecting,
    :max_connections,
    :connections_per_endpoint,
    :max_event_subscribers,
    :max_event_queue,
    :max_refresh_candidates
  ]

  @client_keys [
    :seeds,
    :tls,
    :warm_connections,
    :username,
    :password,
    :client_name,
    :endpoint_validator,
    :endpoint_policy,
    :trusted_hosts,
    :max_batch_items,
    :topology_refresh_timeout
  ]

  @allowed_keys @client_keys ++ @positive_integer_keys ++ ConnectionOptions.keys()
  @max_options length(@allowed_keys) + 1
  @max_client_name_bytes 1_024

  @spec validate(term()) :: :ok | {:error, {atom(), term()}}
  def validate(opts) do
    with :ok <- OptionList.validate(opts, @max_options),
         :ok <- validate_known_options(opts),
         :ok <- ClientSeedOptions.validate(opts),
         :ok <- ConnectionOptions.validate(opts),
         :ok <- ClientCredentialOptions.validate(opts) do
      validators = [
        {:tls, &optional_boolean?/1},
        {:warm_connections, &optional_boolean?/1},
        {:client_name, &optional_client_name?/1},
        {:endpoint_validator, &optional_validator?/1},
        {:endpoint_policy, &optional_endpoint_policy?/1},
        {:trusted_hosts, &optional_hosts?/1},
        {:max_batch_items, &RequestLimits.valid_configured_batch_limit?/1},
        {:topology_refresh_timeout, &optional_positive_timeout?/1}
      ]

      validators =
        validators ++ Enum.map(@positive_integer_keys, &{&1, fn value -> positive?(value) end})

      with :ok <- validate_values(opts, validators), do: validate_credentials(opts)
    end
  end

  @spec merge_url(term(), term()) :: {:ok, keyword()} | {:error, term()}
  def merge_url(url, opts) do
    with :ok <- OptionList.validate(opts, @max_options),
         {:ok, parsed} <- URL.parse(url) do
      merged =
        opts
        |> Keyword.put(:seeds, [{parsed.host, parsed.port}])
        |> Keyword.put(:tls, parsed.tls)
        |> put_url_credential(:username, parsed.username)
        |> put_url_credential(:password, parsed.password)

      {:ok, merged}
    else
      {:error, {:options, value}} ->
        {:error, {:invalid_client_option, :options, value}}

      {:error, _reason} = error ->
        error
    end
  end

  @spec take_url(term()) ::
          {:ok, {term() | nil, keyword()}} | {:error, {:invalid_client_option, :options, term()}}
  def take_url(opts) do
    case OptionList.validate(opts, @max_options) do
      :ok -> {:ok, Keyword.pop(opts, :url)}
      {:error, {:options, value}} -> {:error, {:invalid_client_option, :options, value}}
    end
  end

  defp validate_known_options(opts) do
    case Enum.find(Keyword.keys(opts), &(&1 not in @allowed_keys)) do
      nil -> :ok
      key -> {:error, {key, Keyword.get(opts, key)}}
    end
  end

  @spec positive_integer(keyword(), atom(), pos_integer()) :: pos_integer()
  def positive_integer(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 -> value
      _invalid -> default
    end
  end

  defp validate_values(opts, validators) do
    Enum.reduce_while(validators, :ok, fn {key, validator}, :ok ->
      value = Keyword.get(opts, key)

      if validator.(value) do
        {:cont, :ok}
      else
        {:halt, {:error, {key, value}}}
      end
    end)
  end

  defp validate_credentials(opts) do
    case {Keyword.get(opts, :username), Keyword.get(opts, :password)} do
      {username, nil} when is_binary(username) -> {:error, {:password, nil}}
      _complete_or_password_only -> :ok
    end
  end

  defp optional_boolean?(nil), do: true
  defp optional_boolean?(value), do: is_boolean(value)

  defp optional_client_name?(nil), do: true

  defp optional_client_name?(value),
    do:
      is_binary(value) and value != "" and byte_size(value) <= @max_client_name_bytes and
        String.valid?(value)

  defp optional_validator?(nil), do: true
  defp optional_validator?(value), do: is_function(value, 1)

  defp optional_endpoint_policy?(nil), do: true
  defp optional_endpoint_policy?(value) when value in [:any, :none, :seed_hosts], do: true
  defp optional_endpoint_policy?({:allow_hosts, hosts}), do: EndpointHostList.valid?(hosts)
  defp optional_endpoint_policy?(_value), do: false

  defp optional_hosts?(nil), do: true
  defp optional_hosts?(hosts), do: EndpointHostList.valid?(hosts)

  defp optional_positive_timeout?(nil), do: true
  defp optional_positive_timeout?(value), do: Timeout.finite?(value) and value > 0

  defp positive?(nil), do: true
  defp positive?(value), do: is_integer(value) and value > 0

  defp put_url_credential(opts, _key, nil), do: opts
  defp put_url_credential(opts, key, value), do: Keyword.put_new(opts, key, value)
end
