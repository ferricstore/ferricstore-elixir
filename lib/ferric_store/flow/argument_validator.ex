defmodule FerricStore.Flow.ArgumentValidator do
  @moduledoc false

  @max_bytes FerricStore.RouteKey.max_bytes()

  @spec validate(atom(), atom(), term()) :: :ok | {:error, term()}
  def validate(_operation, _field, value)
      when is_binary(value) and value != "" and byte_size(value) <= @max_bytes,
      do: :ok

  def validate(operation, field, value) when is_binary(value) and value != "" do
    {:error, {:invalid_flow_argument, operation, field, {:maximum_bytes, @max_bytes}, value}}
  end

  def validate(operation, field, value) do
    {:error, {:invalid_flow_argument, operation, field, :expected_nonempty_binary, value}}
  end
end
