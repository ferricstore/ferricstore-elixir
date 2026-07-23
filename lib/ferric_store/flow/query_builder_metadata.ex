defmodule FerricStore.Flow.QueryBuilderMetadata do
  @moduledoc false

  alias FerricStore.Flow.Options.PreparedMap

  @max_key_bytes 64
  @max_state_bytes 64

  def attributes(values), do: metadata_entries(PreparedMap.unwrap(values))

  def state_meta(values) do
    values
    |> PreparedMap.unwrap()
    |> state_meta_entries()
  end

  defp metadata_entries(values) when is_map(values) do
    values
    |> Enum.reduce_while({:ok, [], MapSet.new()}, &metadata_entry/2)
    |> case do
      {:ok, entries, _seen} -> {:ok, Enum.sort_by(entries, &elem(&1, 0))}
      :error -> :error
    end
  end

  defp metadata_entries(_values), do: :error

  defp state_meta_entries(values) when is_map(values) do
    values
    |> Enum.reduce_while({:ok, [], MapSet.new()}, &state_meta_entry/2)
    |> case do
      {:ok, entries, _seen} ->
        {:ok, Enum.sort_by(entries, fn {state, name, _value} -> {state, name} end)}

      :error ->
        :error
    end
  end

  defp state_meta_entries(_values), do: :error

  defp metadata_entry({raw_name, value}, {:ok, entries, seen}) do
    with {:ok, name} <- normalize_key(raw_name),
         false <- MapSet.member?(seen, name) do
      {:cont, {:ok, [{name, value} | entries], MapSet.put(seen, name)}}
    else
      _invalid -> {:halt, :error}
    end
  end

  defp state_meta_entry({raw_state, metadata}, {:ok, entries, seen}) when is_map(metadata) do
    with {:ok, state} <- normalize_state(raw_state),
         false <- MapSet.member?(seen, state),
         {:ok, metadata} <- metadata_entries(PreparedMap.unwrap(metadata)) do
      next_entries =
        Enum.reduce(metadata, entries, fn {name, value}, acc ->
          [{state, name, value} | acc]
        end)

      {:cont, {:ok, next_entries, MapSet.put(seen, state)}}
    else
      _invalid -> {:halt, :error}
    end
  end

  defp state_meta_entry(_entry, _acc), do: {:halt, :error}

  defp normalize_key(name) when is_atom(name),
    do: name |> Atom.to_string() |> normalize_key()

  defp normalize_key(name) when is_binary(name) do
    with true <- String.valid?(name),
         normalized = String.trim(name),
         true <- byte_size(normalized) in 1..@max_key_bytes,
         false <- String.starts_with?(normalized, "__") do
      {:ok, normalized}
    else
      _invalid -> :error
    end
  end

  defp normalize_key(_name), do: :error

  defp normalize_state(state) when is_atom(state),
    do: state |> Atom.to_string() |> normalize_state()

  defp normalize_state(state) when is_binary(state) do
    with true <- String.valid?(state),
         normalized = String.trim(state),
         true <- byte_size(normalized) in 1..@max_state_bytes do
      {:ok, normalized}
    else
      _invalid -> :error
    end
  end

  defp normalize_state(_state), do: :error
end
