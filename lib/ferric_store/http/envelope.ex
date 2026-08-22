defmodule FerricStore.HTTP.Envelope do
  @moduledoc false

  @encoding "ferricstore-json-v1"
  @bytes "$ferricstore_bytes"
  @map "$ferricstore_map"
  @max_depth 64

  @spec encode_commands([term()]) :: {:ok, binary()} | {:error, term()}
  def encode_commands(commands) when is_list(commands) do
    with {:ok, encoded} <- encode_commands_list(commands),
         {:ok, body} <- Jason.encode(%{"encoding" => @encoding, "commands" => encoded}) do
      {:ok, body}
    else
      {:error, %Jason.EncodeError{} = error} ->
        {:error, {:invalid_http_value, Exception.message(error)}}

      {:error, _reason} = error ->
        error
    end
  end

  defp encode_commands_list(commands) do
    Enum.reduce_while(commands, {:ok, []}, fn command, {:ok, acc} ->
      case encode_command(command) do
        {:ok, encoded} -> {:cont, {:ok, [encoded | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> reverse_ok()
  end

  defp encode_command(%{"command" => command, "opcode" => opcode, "payload" => payload})
       when is_binary(command) and is_integer(opcode) and is_map(payload) do
    with {:ok, payload} <- encode_value(payload, 0),
         do: {:ok, %{"command" => command, "opcode" => opcode, "payload" => payload}}
  end

  defp encode_command(command) when is_list(command), do: encode_value(command, 0)
  defp encode_command(_command), do: {:error, {:invalid_http_value, :command}}

  @spec decode(binary()) :: {:ok, map()} | {:error, term()}
  def decode(body) when is_binary(body) do
    case Jason.decode(body) do
      {:ok, envelope} when is_map(envelope) -> decode_value(envelope, 0)
      {:ok, _value} -> {:error, {:invalid_http_response, :expected_object}}
      {:error, error} -> {:error, {:invalid_http_response, Exception.message(error)}}
    end
  end

  defp encode_value(_value, depth) when depth > @max_depth,
    do: {:error, {:invalid_http_value, :maximum_depth}}

  defp encode_value(value, _depth)
       when is_nil(value) or is_boolean(value) or is_integer(value),
       do: {:ok, value}

  defp encode_value(value, _depth) when is_float(value) do
    if finite_float?(value),
      do: {:ok, value},
      else: {:error, {:invalid_http_value, :non_finite_float}}
  end

  defp encode_value(value, _depth) when is_binary(value) do
    if String.valid?(value),
      do: {:ok, value},
      else: {:ok, %{@bytes => Base.encode64(value)}}
  end

  defp encode_value(value, _depth) when is_atom(value), do: {:ok, Atom.to_string(value)}

  defp encode_value(value, depth) when is_list(value), do: encode_list(value, depth + 1)

  defp encode_value(value, depth) when is_tuple(value),
    do: value |> Tuple.to_list() |> encode_list(depth + 1)

  defp encode_value(value, depth) when is_map(value) do
    with {:ok, pairs} <- encode_pairs(Map.to_list(value), depth + 1),
         do: {:ok, %{@map => pairs}}
  end

  defp encode_value(value, _depth), do: {:error, {:invalid_http_value, value}}

  defp encode_list(values, depth), do: encode_list(values, depth, [])

  defp encode_list([], _depth, acc), do: {:ok, Enum.reverse(acc)}

  defp encode_list([value | values], depth, acc) do
    case encode_value(value, depth) do
      {:ok, encoded} -> encode_list(values, depth, [encoded | acc])
      {:error, _reason} = error -> error
    end
  end

  defp encode_list(_improper, _depth, _acc),
    do: {:error, {:invalid_http_value, :improper_list}}

  defp encode_pairs(pairs, depth) do
    Enum.reduce_while(pairs, {:ok, [], MapSet.new()}, fn {key, value}, {:ok, acc, seen} ->
      with {:ok, encoded_key} <- encode_value(key, depth),
           false <- MapSet.member?(seen, encoded_key),
           {:ok, encoded_value} <- encode_value(value, depth) do
        {:cont, {:ok, [[encoded_key, encoded_value] | acc], MapSet.put(seen, encoded_key)}}
      else
        true -> {:halt, {:error, {:invalid_http_value, :duplicate_map_key}}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> reverse_pairs_ok()
  end

  defp decode_value(_value, depth) when depth > @max_depth,
    do: {:error, {:invalid_http_response, :maximum_depth}}

  defp decode_value(%{@bytes => encoded} = marker, _depth)
       when map_size(marker) == 1 and is_binary(encoded) do
    case Base.decode64(encoded) do
      {:ok, value} -> {:ok, value}
      :error -> {:error, {:invalid_http_response, :invalid_base64}}
    end
  end

  defp decode_value(%{@bytes => _invalid} = marker, _depth) when map_size(marker) == 1,
    do: {:error, {:invalid_http_response, :invalid_base64}}

  defp decode_value(%{@map => pairs} = marker, depth) when map_size(marker) == 1,
    do: decode_map(pairs, depth + 1)

  defp decode_value(value, depth) when is_map(value) do
    value
    |> Map.to_list()
    |> decode_plain_map(depth + 1)
  end

  defp decode_value(value, depth) when is_list(value), do: decode_list(value, depth + 1)
  defp decode_value(value, _depth), do: {:ok, value}

  defp decode_map(pairs, depth) when is_list(pairs) do
    Enum.reduce_while(pairs, {:ok, %{}}, fn
      [key, value], {:ok, acc} ->
        with {:ok, key} <- decode_value(key, depth),
             false <- Map.has_key?(acc, key),
             {:ok, value} <- decode_value(value, depth) do
          {:cont, {:ok, Map.put(acc, key, value)}}
        else
          true -> {:halt, {:error, {:invalid_http_response, :duplicate_map_key}}}
          {:error, _reason} = error -> {:halt, error}
        end

      _invalid, _acc ->
        {:halt, {:error, {:invalid_http_response, :invalid_map_marker}}}
    end)
  end

  defp decode_map(_pairs, _depth), do: {:error, {:invalid_http_response, :invalid_map_marker}}

  defp decode_plain_map(pairs, depth) do
    Enum.reduce_while(pairs, {:ok, %{}}, fn {key, value}, {:ok, acc} ->
      case decode_value(value, depth) do
        {:ok, decoded} -> {:cont, {:ok, Map.put(acc, key, decoded)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp decode_list(values, depth) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, acc} ->
      case decode_value(value, depth) do
        {:ok, decoded} -> {:cont, {:ok, [decoded | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> reverse_ok()
  end

  defp reverse_ok({:ok, values}), do: {:ok, Enum.reverse(values)}
  defp reverse_ok({:error, _reason} = error), do: error

  defp reverse_pairs_ok({:ok, values, _seen}), do: {:ok, Enum.reverse(values)}
  defp reverse_pairs_ok({:error, _reason} = error), do: error

  defp finite_float?(value) do
    case Jason.encode(value) do
      {:ok, _encoded} -> true
      {:error, _reason} -> false
    end
  end
end
