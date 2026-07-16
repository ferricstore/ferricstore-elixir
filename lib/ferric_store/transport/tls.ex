defmodule FerricStore.Transport.TLS do
  @moduledoc false

  alias FerricStore.Transport.CACerts

  @spec options(map() | keyword()) :: keyword()
  def options(config) when is_map(config) or is_list(config) do
    base = [mode: :binary, active: false, packet: :raw, nodelay: true]

    if verify?(config) do
      hostname = config |> server_name() |> normalize_hostname()

      base
      |> Keyword.merge(
        verify: :verify_peer,
        server_name_indication: hostname,
        customize_hostname_check: [
          match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
        ]
      )
      |> put_ca_options(config)
    else
      Keyword.put(base, :verify, :verify_none)
    end
  end

  defp verify?(config) do
    option(config, :verify, true) != false and option(config, :tls_verify, true) != false
  end

  defp server_name(config) do
    option(config, :server_name) ||
      option(config, :host) ||
      raise ArgumentError, "TLS configuration requires :host or :server_name"
  end

  defp put_ca_options(options, config) do
    cond do
      cacertfile = option(config, :cacertfile) ->
        Keyword.put(options, :cacertfile, cacertfile)

      cacerts = option(config, :cacerts) ->
        Keyword.put(options, :cacerts, CACerts.certificates(cacerts))

      function_exported?(:public_key, :cacerts_get, 0) ->
        Keyword.put(options, :cacerts, :public_key.cacerts_get())

      true ->
        options
    end
  end

  defp option(config, key, default \\ nil)

  defp option(config, key, default) when is_list(config),
    do: Keyword.get(config, key, default)

  defp option(config, key, default) when is_map(config),
    do: Map.get(config, key, Map.get(config, Atom.to_string(key), default))

  defp normalize_hostname(value) when is_binary(value), do: String.to_charlist(value)
  defp normalize_hostname(value) when is_list(value), do: value
end
