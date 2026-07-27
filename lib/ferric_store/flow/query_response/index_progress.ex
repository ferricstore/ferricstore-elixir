defmodule FerricStore.Flow.QueryResponse.IndexProgress do
  @moduledoc false

  alias FerricStore.Flow.QueryResponse.Validation
  alias FerricStore.Types

  @maximum_unsigned_64 18_446_744_073_709_551_615

  def decode(value, section, allowed_phases) when is_map(value) do
    with {:ok, scope} <- choice(value, "scope", ["catalog_build"], {section, :scope}),
         {:ok, phase_counts} <- phase_counts(Types.get(value, "phase_counts"), section),
         {:ok, current_phases} <-
           phases(Types.get(value, "current_phases"), section, allowed_phases),
         {:ok, completed_shards} <- Validation.unsigned(value, "completed_shards"),
         {:ok, total_shards} <- Validation.positive_unsigned(value, "total_shards"),
         true <- completed_shards <= total_shards do
      {:ok,
       %{
         scope: scope,
         phase_counts: phase_counts,
         current_phases: current_phases,
         completed_shards: completed_shards,
         total_shards: total_shards,
         raw: value
       }}
    else
      false -> Validation.invalid(section, value)
      {:error, _reason} = error -> error
    end
  end

  def decode(value, section, _allowed), do: Validation.invalid(section, value)

  def phase_counts(value, section) when is_map(value) and map_size(value) <= 16 do
    Enum.reduce_while(value, {:ok, %{}}, fn {phase, count}, {:ok, acc} ->
      if is_binary(phase) and phase != "" and byte_size(phase) <= 64 and String.valid?(phase) and
           is_integer(count) and count >= 0 and count <= @maximum_unsigned_64 do
        {:cont, {:ok, Map.put(acc, phase, count)}}
      else
        {:halt, Validation.invalid({section, :phase_counts}, value)}
      end
    end)
  end

  def phase_counts(value, section), do: Validation.invalid({section, :phase_counts}, value)

  def phases(value, section, allowed) do
    with {:ok, phases} <-
           unique_texts(value, length(allowed), 64, {section, :current_phases}),
         true <- Enum.all?(phases, &(&1 in allowed)) do
      {:ok, phases}
    else
      false -> Validation.invalid({section, :current_phases}, value)
      {:error, _reason} = error -> error
    end
  end

  defp unique_texts(value, maximum, maximum_bytes, field)
       when is_list(value) and length(value) <= maximum do
    with true <-
           Enum.all?(value, fn text ->
             is_binary(text) and text != "" and byte_size(text) <= maximum_bytes and
               String.valid?(text)
           end),
         true <- length(value) == MapSet.size(MapSet.new(value)) do
      {:ok, value}
    else
      false -> Validation.invalid(field, value)
    end
  end

  defp unique_texts(value, _maximum, _maximum_bytes, field),
    do: Validation.invalid(field, value)

  defp choice(value, field, choices, error_field) do
    actual = Types.get(value, field)
    if actual in choices, do: {:ok, actual}, else: Validation.invalid(error_field, actual)
  end
end
