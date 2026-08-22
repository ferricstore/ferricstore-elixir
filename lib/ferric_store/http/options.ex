defmodule FerricStore.HTTP.Options do
  @moduledoc false

  alias FerricStore.HTTP.PoolSupervisor

  @default_timeout 30_000
  @default_request_bytes 1024 * 1024
  @default_response_bytes 16 * 1024 * 1024
  @default_batch_items 1_000
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
    with :ok <- supported_options(opts),
         {:ok, uri} <- parse_url(url),
         {:ok, http2} <- boolean(opts, :http2, false),
         {:ok, headers} <- headers(uri.scheme, opts),
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

  defp parse_url(url) do
    with {:ok, uri} <- URI.new(url),
         true <- uri.scheme in ["http", "https"] || {:error, {:invalid_url_scheme, uri.scheme}},
         true <- (is_binary(uri.host) and uri.host != "") || {:error, {:invalid_http_url, :host}},
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
      value when is_integer(value) and value > 0 -> {:ok, value}
      value -> {:error, {:invalid_http_option, :timeout, value}}
    end
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

  defp headers(scheme, opts) do
    with {:ok, headers} <- normalize_headers(Keyword.get(opts, :headers, %{})),
         {:ok, authorization} <- authorization(scheme, opts, headers) do
      {:ok, put_authorization(headers, authorization)}
    end
  end

  defp normalize_headers(headers) when is_map(headers),
    do: normalize_headers(Map.to_list(headers))

  defp normalize_headers(headers) when is_list(headers) do
    Enum.reduce_while(headers, {:ok, []}, fn
      {name, value}, {:ok, acc} when is_binary(name) and is_binary(value) ->
        if safe_header?(name, value),
          do: {:cont, {:ok, [{String.downcase(name), value} | acc]}},
          else: {:halt, {:error, {:invalid_http_header, name}}}

      invalid, _acc ->
        {:halt, {:error, {:invalid_http_header, invalid}}}
    end)
  end

  defp normalize_headers(invalid), do: {:error, {:invalid_http_headers, invalid}}

  defp authorization(scheme, opts, headers) do
    bearer = Keyword.get(opts, :bearer_token)
    username = Keyword.get(opts, :username)
    password = Keyword.get(opts, :password)
    custom? = List.keymember?(headers, "authorization", 0)

    cond do
      Enum.count([custom?, is_binary(bearer), not is_nil(username) or not is_nil(password)], & &1) >
          1 ->
        {:error, {:invalid_http_credentials, :mutually_exclusive}}

      is_binary(bearer) and safe_value?(bearer) ->
        {:ok, "Bearer " <> bearer}

      is_binary(bearer) ->
        {:error, {:invalid_http_credentials, :bearer_token}}

      not is_nil(username) or not is_nil(password) ->
        basic_authorization(scheme, username, password)

      true ->
        {:ok, nil}
    end
  end

  defp basic_authorization("https", username, password)
       when (is_nil(username) or is_binary(username)) and is_binary(password) do
    username = username || "default"

    if username != "" and not String.contains?(username, ":") and safe_value?(username) and
         safe_value?(password),
       do: {:ok, "Basic " <> Base.encode64(username <> ":" <> password)},
       else: {:error, {:invalid_http_credentials, :basic}}
  end

  defp basic_authorization("http", _username, _password),
    do: {:error, {:invalid_http_credentials, :https_required}}

  defp basic_authorization(_scheme, _username, _password),
    do: {:error, {:invalid_http_credentials, :username_password}}

  defp put_authorization(headers, nil), do: headers

  defp put_authorization(headers, value),
    do: List.keystore(headers, "authorization", 0, {"authorization", value})

  defp safe_header?(name, value), do: name != "" and safe_value?(name) and safe_value?(value)
  defp safe_value?(value), do: not String.contains?(value, ["\r", "\n"])
end
