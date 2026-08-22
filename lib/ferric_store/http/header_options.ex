defmodule FerricStore.HTTP.HeaderOptions do
  @moduledoc false

  @reserved MapSet.new(
              ~w(connection content-length host keep-alive te trailer transfer-encoding upgrade)
            )

  @spec build(binary(), keyword()) :: {:ok, [{binary(), binary()}]} | {:error, term()}
  def build(scheme, opts) do
    with {:ok, headers} <- normalize(Keyword.get(opts, :headers, %{})),
         {:ok, authorization} <- authorization(scheme, opts, headers) do
      {:ok, put_authorization(headers, authorization)}
    end
  end

  defp normalize(headers) when is_map(headers), do: normalize(Map.to_list(headers))

  defp normalize(headers) when is_list(headers), do: normalize_entries(headers, [], MapSet.new())

  defp normalize(invalid), do: {:error, {:invalid_http_headers, invalid}}

  defp normalize_entries([], acc, _seen), do: {:ok, acc}

  defp normalize_entries([{name, value} | headers], acc, seen)
       when is_binary(name) and is_binary(value) do
    case normalize_header(name, value, seen) do
      {:ok, normalized} ->
        normalize_entries(headers, [{normalized, value} | acc], MapSet.put(seen, normalized))

      {:error, _reason} = error ->
        error
    end
  end

  defp normalize_entries([invalid | _headers], _acc, _seen),
    do: {:error, {:invalid_http_header, invalid}}

  defp normalize_entries(invalid_tail, _acc, _seen),
    do: {:error, {:invalid_http_headers, invalid_tail}}

  defp normalize_header(name, value, seen) do
    normalized = if header_name?(name), do: String.downcase(name)

    cond do
      not safe_header?(name, value) ->
        {:error, {:invalid_http_header, name}}

      MapSet.member?(@reserved, normalized) ->
        {:error, {:invalid_http_header, {:reserved, normalized}}}

      MapSet.member?(seen, normalized) ->
        {:error, {:invalid_http_header, {:duplicate, normalized}}}

      true ->
        {:ok, normalized}
    end
  end

  defp authorization(scheme, opts, headers) do
    bearer = Keyword.get(opts, :bearer_token)
    username = Keyword.get(opts, :username)
    password = Keyword.get(opts, :password)
    custom? = List.keymember?(headers, "authorization", 0)

    cond do
      multiple_credentials?(custom?, bearer, username, password) ->
        {:error, {:invalid_http_credentials, :mutually_exclusive}}

      is_binary(bearer) ->
        bearer_authorization(bearer)

      not is_nil(username) or not is_nil(password) ->
        basic_authorization(scheme, username, password)

      true ->
        {:ok, nil}
    end
  end

  defp multiple_credentials?(custom?, bearer, username, password) do
    kinds = [custom?, is_binary(bearer), not is_nil(username) or not is_nil(password)]
    Enum.count(kinds, & &1) > 1
  end

  defp bearer_authorization(bearer) do
    if bearer != "" and safe_value?(bearer),
      do: {:ok, "Bearer " <> bearer},
      else: {:error, {:invalid_http_credentials, :bearer_token}}
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

  defp safe_header?(name, value), do: header_name?(name) and safe_value?(value)

  defp header_name?(name) when name != "" do
    name
    |> :binary.bin_to_list()
    |> Enum.all?(&header_name_byte?/1)
  end

  defp header_name?(""), do: false

  defp header_name_byte?(byte) when byte in ?0..?9, do: true
  defp header_name_byte?(byte) when byte in ?A..?Z, do: true
  defp header_name_byte?(byte) when byte in ?a..?z, do: true
  defp header_name_byte?(byte), do: byte in ~c"!#$%&'*+-.^_`|~"

  defp safe_value?(value) do
    value
    |> :binary.bin_to_list()
    |> Enum.all?(fn byte -> byte == ?\t or (byte >= 32 and byte != 127) end)
  end
end
