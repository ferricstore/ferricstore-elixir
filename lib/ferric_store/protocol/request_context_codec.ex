defmodule FerricStore.Protocol.RequestContextCodec do
  @moduledoc false

  alias FerricStore.{FailureFormatter, Protocol.RequestContextScopes}

  @spec put(map(), keyword()) :: map()
  def put(payload, opts) when is_map(payload) and is_list(opts) do
    case normalize(Keyword.get(opts, :request_context)) do
      nil -> payload
      context -> Map.put(payload, "request_context", context)
    end
  end

  @spec put_result(map(), keyword()) ::
          {:ok, map()} | {:error, {:invalid_request_context, binary()}}
  def put_result(payload, opts) do
    {:ok, put(payload, opts)}
  rescue
    error ->
      {:error,
       {:invalid_request_context,
        FailureFormatter.exception_message(error, "request context validation failed")}}
  catch
    kind, reason ->
      {:error, {:invalid_request_context, FailureFormatter.inspect_term({kind, reason})}}
  end

  defp normalize(nil), do: nil

  defp normalize(%{} = context) do
    %{}
    |> put_context_value("subject", context_value(context, "subject", :subject))
    |> put_context_value("tenant", context_value(context, "tenant", :tenant))
    |> put_context_scopes(context_value(context, "scopes", :scopes))
    |> empty_to_nil()
  end

  defp normalize(_context), do: raise(ArgumentError, "request context must be a map")

  defp put_context_value(payload, _key, value) when value in [nil, ""], do: payload

  defp put_context_value(payload, key, value) when is_binary(value),
    do: Map.put(payload, key, value)

  defp put_context_value(_payload, key, value),
    do:
      raise(
        ArgumentError,
        "request context field #{key} must be a binary, got: #{inspect(value)}"
      )

  defp put_context_scopes(payload, scopes) do
    scopes = RequestContextScopes.normalize(scopes)

    case scopes do
      [] -> payload
      scopes -> Map.put(payload, "scopes", scopes)
    end
  end

  defp context_value(context, string_key, atom_key) do
    case {Map.fetch(context, string_key), Map.fetch(context, atom_key)} do
      {{:ok, _string_value}, {:ok, _atom_value}} ->
        raise ArgumentError, "duplicate request context field #{inspect(string_key)}"

      {{:ok, value}, :error} ->
        value

      {:error, {:ok, value}} ->
        value

      {:error, :error} ->
        nil
    end
  end

  defp empty_to_nil(map) when map == %{}, do: nil
  defp empty_to_nil(map), do: map
end
