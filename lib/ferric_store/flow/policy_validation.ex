defmodule FerricStore.Flow.PolicyValidation do
  @moduledoc false

  alias FerricStore.Types

  def option_map(value) when is_map(value), do: Types.normalize_map(value)
  def option_map(value) when is_list(value), do: value |> Map.new() |> Types.normalize_map()

  def allowed(:error, _allowed, _path), do: :ok
  def allowed({:ok, value}, allowed, path), do: if(value in allowed, do: :ok, else: error(path))

  def bounded_integer(:error, _min, _max, _path), do: :ok

  def bounded_integer({:ok, value}, min, max, _path)
      when is_integer(value) and value >= min and value <= max,
      do: :ok

  def bounded_integer({:ok, _value}, _min, _max, path), do: error(path)

  def error(path), do: {:error, {:invalid_policy_option, path}}
end
