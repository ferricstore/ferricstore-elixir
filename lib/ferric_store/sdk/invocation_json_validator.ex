defmodule FerricStore.SDK.InvocationJSONValidator do
  @moduledoc false

  alias Jason.OrderedObject

  def validate(%OrderedObject{values: values}),
    do: validate_fields(values, MapSet.new())

  def validate(values) when is_list(values), do: validate_values(values)
  def validate(_scalar), do: :ok

  defp validate_fields([], _seen), do: :ok

  defp validate_fields([{key, value} | fields], seen) do
    if MapSet.member?(seen, key) do
      {:error, :duplicate_json_object_key}
    else
      with :ok <- validate(value) do
        validate_fields(fields, MapSet.put(seen, key))
      end
    end
  end

  defp validate_values([]), do: :ok

  defp validate_values([value | values]) do
    with :ok <- validate(value), do: validate_values(values)
  end
end
