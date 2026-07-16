defmodule FerricStore.RouteKey do
  @moduledoc false

  alias FerricStore.{RequestOptions, RouteKeyValidator}

  @type resolution ::
          :none
          | {:ok, binary()}
          | {:error,
             {:invalid_route_key, term()}
             | {:duplicate_route_field, binary()}
             | {:invalid_request_option, atom(), term()}
             | {:conflicting_route_options, [atom()]}}

  @spec resolve(term(), term(), [atom()], [binary() | {binary(), atom()}]) :: resolution()
  def resolve(payload, opts, option_keys, payload_fields)
      when is_list(option_keys) and is_list(payload_fields) do
    case from_options(opts, option_keys) do
      :none when is_map(payload) -> from_payload(payload, payload_fields)
      :none -> :none
      resolution -> resolution
    end
  end

  @spec from_options(term(), [atom()]) :: resolution()
  def from_options(opts, keys) when is_list(keys) do
    case RequestOptions.validate(opts) do
      :ok -> find_option(opts, keys)
      {:error, {key, value}} -> {:error, {:invalid_request_option, key, value}}
    end
  end

  @spec from_payload(map(), [binary() | {binary(), atom()}]) :: resolution()
  def from_payload(payload, fields) when is_map(payload) and is_list(fields) do
    find_payload_field(payload, fields, :none)
  end

  @spec ensure_unambiguous_payload_fields(map(), [{binary(), atom()}]) ::
          :ok | {:error, {:duplicate_route_field, binary()}}
  def ensure_unambiguous_payload_fields(payload, fields)
      when is_map(payload) and is_list(fields) do
    Enum.reduce_while(fields, :ok, fn field, :ok ->
      case fetch_field(payload, field) do
        {:error, _reason} = error -> {:halt, error}
        _missing_or_value -> {:cont, :ok}
      end
    end)
  end

  defdelegate max_bytes, to: RouteKeyValidator

  @spec validate(term()) :: {:ok, binary()} | {:error, {:invalid_route_key, term()}}
  defdelegate validate(value), to: RouteKeyValidator

  defp find_option(opts, keys), do: find_option(opts, keys, nil)

  defp find_option(_opts, [], nil), do: :none
  defp find_option(_opts, [], {_key, value}), do: validate(value)

  defp find_option(opts, [key | keys], found) do
    case {Keyword.fetch(opts, key), found} do
      {:error, found} ->
        find_option(opts, keys, found)

      {{:ok, value}, nil} ->
        find_option(opts, keys, {key, value})

      {{:ok, _value}, {first_key, _first_value}} ->
        {:error, {:conflicting_route_options, [first_key, key]}}
    end
  end

  defp find_payload_field(_payload, [], :none), do: :none
  defp find_payload_field(_payload, [], {:found, value}), do: validate(value)

  defp find_payload_field(payload, [field | fields], found) do
    case fetch_field(payload, field) do
      {:ok, value} when found == :none ->
        find_payload_field(payload, fields, {:found, value})

      {:ok, _value} ->
        find_payload_field(payload, fields, found)

      :error ->
        find_payload_field(payload, fields, found)

      {:error, _reason} = error ->
        error
    end
  end

  defp fetch_field(payload, {string_key, atom_key}) do
    case {Map.fetch(payload, string_key), Map.fetch(payload, atom_key)} do
      {{:ok, _value}, {:ok, _other_value}} ->
        {:error, {:duplicate_route_field, string_key}}

      {{:ok, value}, :error} ->
        {:ok, value}

      {:error, {:ok, value}} ->
        {:ok, value}

      {:error, :error} ->
        :error
    end
  end

  defp fetch_field(payload, key), do: Map.fetch(payload, key)
end
