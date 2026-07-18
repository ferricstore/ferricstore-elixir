defmodule FerricStore.Flow.PolicyIndexValidator do
  @moduledoc false

  alias FerricStore.BoundedList
  alias FerricStore.Flow.{MaxActive, PolicyValidation}

  def validate(options) do
    with :ok <- validate_max_active_ms(Map.fetch(options, "max_active_ms")),
         :ok <- validate_attributes(Map.fetch(options, "indexed_attributes")) do
      validate_state_meta(Map.fetch(options, "indexed_state_meta"))
    end
  end

  defp validate_max_active_ms(:error), do: :ok

  defp validate_max_active_ms({:ok, value}) do
    if MaxActive.valid?(value),
      do: :ok,
      else: PolicyValidation.error("max_active_ms")
  end

  defp validate_attributes(:error), do: :ok
  defp validate_attributes({:ok, nil}), do: :ok

  defp validate_attributes({:ok, values}) when is_list(values) do
    with {:ok, _count} <- BoundedList.count(values, 3),
         :ok <- validate_unique_keys(values, MapSet.new()) do
      :ok
    else
      _invalid -> PolicyValidation.error("indexed_attributes")
    end
  end

  defp validate_attributes({:ok, _value}),
    do: PolicyValidation.error("indexed_attributes")

  defp validate_state_meta(:error), do: :ok
  defp validate_state_meta({:ok, value}) when value in [nil, "", []], do: :ok

  defp validate_state_meta({:ok, value}) when is_binary(value) or is_atom(value),
    do: validate_key_result(value, "indexed_state_meta")

  defp validate_state_meta({:ok, values}) when is_list(values) do
    with {:ok, _count} <- BoundedList.count(values, 1),
         :ok <- validate_unique_keys(values, MapSet.new()) do
      :ok
    else
      _invalid -> PolicyValidation.error("indexed_state_meta")
    end
  end

  defp validate_state_meta({:ok, _value}),
    do: PolicyValidation.error("indexed_state_meta")

  defp validate_unique_keys([], _seen), do: :ok

  defp validate_unique_keys([value | values], seen) do
    with {:ok, key} <- normalized_key(value),
         false <- MapSet.member?(seen, key) do
      validate_unique_keys(values, MapSet.put(seen, key))
    else
      _invalid -> :error
    end
  end

  defp validate_key_result(value, path) do
    case normalized_key(value) do
      {:ok, _key} -> :ok
      :error -> PolicyValidation.error(path)
    end
  end

  defp normalized_key(value) when is_atom(value),
    do: value |> Atom.to_string() |> normalized_key()

  defp normalized_key(value) when is_binary(value) do
    if byte_size(value) <= 64 and String.valid?(value) do
      value = String.trim(value)

      if value != "" and not String.starts_with?(value, "__"),
        do: {:ok, value},
        else: :error
    else
      :error
    end
  end

  defp normalized_key(_value), do: :error
end
