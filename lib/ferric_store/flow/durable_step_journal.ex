defmodule FerricStore.Flow.DurableStepJournal do
  @moduledoc false

  alias FerricStore.Types

  @result_prefix "__ferricstore_step__:sha256:"

  @spec result_name(binary()) :: binary()
  def result_name(name) when is_binary(name) do
    @result_prefix <> Base.encode16(:crypto.hash(:sha256, name), case: :lower)
  end

  @spec committed_ref(map(), binary()) :: {:ok, binary()} | :missing | {:error, term()}
  def committed_ref(record, name) do
    record
    |> Types.get(:value_refs, %{})
    |> normalize_committed_ref(name)
  end

  defp normalize_committed_ref(refs, name) when is_map(refs) do
    case fetch_ref(refs, name) do
      :error ->
        :missing

      {:ok, ref} when is_binary(ref) and ref != "" ->
        {:ok, ref}

      {:ok, %{"ref" => ref}} when is_binary(ref) and ref != "" ->
        {:ok, ref}

      {:ok, %{ref: ref}} when is_binary(ref) and ref != "" ->
        {:ok, ref}

      {:ok, _invalid} ->
        {:error, {:invalid_flow_response, %{operation: :step, reason: :invalid_value_ref}}}
    end
  end

  defp normalize_committed_ref(_invalid, _name),
    do: {:error, {:invalid_flow_response, %{operation: :step, reason: :invalid_value_refs}}}

  defp fetch_ref(refs, name) do
    case Map.fetch(refs, name) do
      {:ok, _value} = found -> found
      :error -> fetch_atom_ref(refs, name)
    end
  end

  defp fetch_atom_ref(refs, name) do
    Map.fetch(refs, String.to_existing_atom(name))
  rescue
    ArgumentError -> :error
  end
end
