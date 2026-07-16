defmodule FerricStore.URL do
  @moduledoc false

  @default_ports %{
    "ferric" => 6388,
    "ferrics" => 6389,
    "ferric+tls" => 6389
  }
  @max_url_bytes 8_192

  @type parsed :: %{
          host: binary(),
          port: 1..65_535,
          tls: boolean(),
          username: binary() | nil,
          password: binary() | nil
        }

  @spec parse(binary()) :: {:ok, parsed()} | {:error, term()}
  def parse(url) when is_binary(url) and byte_size(url) <= @max_url_bytes do
    uri = URI.parse(url)
    scheme = normalize_scheme(uri.scheme)

    with {:ok, tls, default_port} <- scheme_options(scheme),
         true <- valid_host?(uri.host),
         true <- valid_target?(uri),
         port when is_integer(port) <- uri.port || default_port,
         true <- port in 1..65_535,
         {:ok, username, password} <- parse_userinfo(uri.userinfo) do
      {:ok,
       %{
         host: uri.host,
         port: port,
         tls: tls,
         username: username,
         password: password
       }}
    else
      false -> {:error, :invalid_url}
      nil -> {:error, :invalid_url}
      {:error, _reason} = error -> error
    end
  rescue
    _error -> {:error, :invalid_url}
  end

  def parse(_url), do: {:error, :invalid_url}

  defp normalize_scheme(scheme) when is_binary(scheme), do: String.downcase(scheme)
  defp normalize_scheme(scheme), do: scheme

  defp scheme_options("ferric"), do: {:ok, false, @default_ports["ferric"]}
  defp scheme_options("ferrics"), do: {:ok, true, @default_ports["ferrics"]}
  defp scheme_options("ferric+tls"), do: {:ok, true, @default_ports["ferric+tls"]}

  defp scheme_options(scheme) when is_binary(scheme),
    do: {:error, {:invalid_url_scheme, scheme}}

  defp scheme_options(_scheme), do: {:error, :invalid_url}

  defp valid_host?(host), do: is_binary(host) and host != ""

  defp valid_target?(%URI{path: path, query: nil, fragment: nil}) when path in [nil, "", "/"],
    do: true

  defp valid_target?(%URI{}), do: false

  defp parse_userinfo(nil), do: {:ok, nil, nil}

  defp parse_userinfo(userinfo) do
    case String.split(userinfo, ":", parts: 2) do
      [username, password] ->
        {:ok, username |> URI.decode() |> empty_username(), URI.decode(password)}

      [username] ->
        {:ok, username |> URI.decode() |> empty_username(), nil}
    end
  rescue
    _error -> {:error, :invalid_url}
  end

  defp empty_username(""), do: nil
  defp empty_username(username), do: username
end
