defmodule FerricStore.HTTP.Options do
  @moduledoc false

  alias FerricStore.HTTP.{HeaderOptions, PoolSupervisor}
  alias FerricStore.{OptionList, Timeout}

  @default_timeout 30_000
  @default_request_bytes 1024 * 1024
  @default_response_bytes 16 * 1024 * 1024
  @default_batch_items 1_000
  @max_options 32
  @keys [
    :bearer_token,
    :headers,
    :http2,
    :max_batch_items,
    :max_concurrent_requests,
    :max_connections,
    :max_request_bytes,
    :max_response_bytes,
    :password,
    :timeout,
    :username
  ]

  @enforce_keys [:base_url, :command_url, :headers, :pool]
  defstruct @enforce_keys ++
              [
                timeout: @default_timeout,
                max_request_bytes: @default_request_bytes,
                max_response_bytes: @default_response_bytes,
                max_batch_items: @default_batch_items,
                max_concurrent_requests: 1,
                http2: false
              ]

  @type t :: %__MODULE__{
          base_url: binary(),
          command_url: binary(),
          headers: [{binary(), binary()}],
          pool: atom(),
          timeout: timeout(),
          max_request_bytes: pos_integer(),
          max_response_bytes: pos_integer(),
          max_batch_items: pos_integer(),
          max_concurrent_requests: pos_integer(),
          http2: boolean()
        }

  @spec new(binary(), keyword()) :: {:ok, t()} | {:error, term()}
  def new(url, opts) when is_binary(url) and is_list(opts) do
    with :ok <- option_list(opts),
         :ok <- supported_options(opts),
         {:ok, uri} <- parse_url(url),
         {:ok, http2} <- boolean(opts, :http2, false),
         {:ok, headers} <- HeaderOptions.build(uri.scheme, opts),
         {:ok, timeout} <- timeout(opts),
         {:ok, max_connections} <- positive(opts, :max_connections, 1),
         {:ok, max_concurrent} <- concurrent_limit(opts, http2, max_connections),
         {:ok, max_request} <- positive(opts, :max_request_bytes, @default_request_bytes),
         {:ok, max_response} <- positive(opts, :max_response_bytes, @default_response_bytes),
         {:ok, max_batch} <- positive(opts, :max_batch_items, @default_batch_items),
         {:ok, pool} <- PoolSupervisor.ensure_started(http2) do
      base_url = uri |> URI.to_string() |> String.trim_trailing("/")

      {:ok,
       %__MODULE__{
         base_url: base_url,
         command_url: base_url <> "/v1/commands",
         headers: headers,
         pool: pool,
         timeout: timeout,
         max_request_bytes: max_request,
         max_response_bytes: max_response,
         max_batch_items: max_batch,
         max_concurrent_requests: max_concurrent,
         http2: http2
       }}
    end
  end

  def new(_url, _opts), do: {:error, {:invalid_http_options, :expected_url_and_keyword_list}}

  defp option_list(opts) do
    case OptionList.validate(opts, @max_options) do
      :ok -> :ok
      {:error, {:options, reason}} -> {:error, {:invalid_http_options, reason}}
    end
  end

  defp parse_url(url) do
    with {:ok, uri} <- URI.new(url),
         true <- uri.scheme in ["http", "https"] || {:error, {:invalid_url_scheme, uri.scheme}},
         true <- (is_binary(uri.host) and uri.host != "") || {:error, {:invalid_http_url, :host}},
         true <- uri.port in 1..65_535 || {:error, {:invalid_http_url, :port}},
         true <- is_nil(uri.userinfo) || {:error, {:invalid_http_url, :userinfo}},
         true <- is_nil(uri.query) || {:error, {:invalid_http_url, :query}},
         true <- is_nil(uri.fragment) || {:error, {:invalid_http_url, :fragment}} do
      {:ok, uri}
    end
  end

  defp supported_options(opts) do
    case Enum.find(opts, fn {key, _value} -> key not in @keys end) do
      nil -> :ok
      {key, value} -> {:error, {:invalid_http_option, key, value}}
    end
  end

  defp timeout(opts) do
    case Keyword.get(opts, :timeout, @default_timeout) do
      :infinity -> {:ok, :infinity}
      value -> validate_timeout(value)
    end
  end

  defp validate_timeout(value) do
    if Timeout.positive?(value),
      do: {:ok, value},
      else: {:error, {:invalid_http_option, :timeout, value}}
  end

  defp positive(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 -> {:ok, value}
      value -> {:error, {:invalid_http_option, key, value}}
    end
  end

  defp boolean(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_boolean(value) -> {:ok, value}
      value -> {:error, {:invalid_http_option, key, value}}
    end
  end

  defp concurrent_limit(opts, http2, connections) do
    default = if http2, do: 100, else: connections
    positive(opts, :max_concurrent_requests, default)
  end
end
