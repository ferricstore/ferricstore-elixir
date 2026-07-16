defmodule FerricStore.SDK.InvocationJSONInputValidator do
  @moduledoc false

  @max_depth 64

  @spec validate(term()) :: :ok | {:error, :not_json_encodable}
  def validate(value), do: validate_value(value, 0)

  defp validate_value(value, _depth)
       when is_binary(value) or is_integer(value) or is_float(value) or is_boolean(value) or
              is_nil(value),
       do: :ok

  defp validate_value(%_struct{}, _depth), do: {:error, :not_json_encodable}

  defp validate_value(value, depth) when is_map(value) and depth < @max_depth do
    Enum.reduce_while(value, :ok, fn {key, nested}, :ok ->
      with :ok <- validate_key(key),
           :ok <- validate_value(nested, depth + 1) do
        {:cont, :ok}
      else
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp validate_value([], _depth), do: :ok

  defp validate_value([value | values], depth) when depth < @max_depth do
    with :ok <- validate_value(value, depth + 1),
         do: validate_list(values, depth)
  end

  defp validate_value(_value, _depth), do: {:error, :not_json_encodable}

  defp validate_list([], _depth), do: :ok

  defp validate_list([value | values], depth) do
    with :ok <- validate_value(value, depth + 1),
         do: validate_list(values, depth)
  end

  defp validate_list(_improper_tail, _depth), do: {:error, :not_json_encodable}

  defp validate_key(key) when is_binary(key) or is_atom(key), do: :ok
  defp validate_key(_key), do: {:error, :not_json_encodable}
end
