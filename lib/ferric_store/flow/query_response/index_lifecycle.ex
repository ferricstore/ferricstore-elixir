defmodule FerricStore.Flow.QueryResponse.IndexLifecycle do
  @moduledoc false

  alias FerricStore.Flow.{
    QueryIndexBuild,
    QueryIndexCoverage,
    QueryIndexRetirement,
    QueryIndexValidation
  }

  alias FerricStore.Flow.QueryResponse.{IndexProgress, Validation}
  alias FerricStore.Types

  @build_phases ~w(pending snapshot backfill done)
  @validation_phases ~w(pending source index counter cleanup done)
  @retirement_phases ~w(pending fence index counter reverse cleanup done)

  def coverage(value) when is_map(value) do
    with {:ok, complete} <- Validation.unsigned(value, "complete_shards"),
         {:ok, total} <- Validation.positive_unsigned(value, "total_shards"),
         true <- complete <= total,
         {:ok, validation} <-
           choice(value, "validation", ~w(pending passed failed), :coverage_validation) do
      {:ok,
       %QueryIndexCoverage{
         complete_shards: complete,
         total_shards: total,
         validation: validation,
         raw: value
       }}
    else
      false -> Validation.invalid(:coverage, value)
      {:error, _reason} = error -> error
    end
  end

  def coverage(value), do: Validation.invalid(:coverage, value)

  def build(value) do
    with {:ok, progress} <- IndexProgress.decode(value, :build, @build_phases),
         {:ok, scanned_records} <- Validation.unsigned(value, "scanned_records"),
         {:ok, written_entries} <- Validation.unsigned(value, "written_entries"),
         {:ok, written_bytes} <- Validation.unsigned(value, "written_bytes") do
      {:ok,
       struct!(
         QueryIndexBuild,
         Map.merge(progress, %{
           scanned_records: scanned_records,
           written_entries: written_entries,
           written_bytes: written_bytes
         })
       )}
    end
  end

  def validation(value) do
    with {:ok, progress} <- IndexProgress.decode(value, :validation, @validation_phases),
         {:ok, status} <-
           choice(value, "status", ~w(pending passed failed), :validation_status),
         {:ok, checked_records} <- Validation.unsigned(value, "checked_records"),
         {:ok, checked_entries} <- Validation.unsigned(value, "checked_entries"),
         {:ok, mismatches} <- Validation.unsigned(value, "mismatches"),
         {:ok, failure_reason} <- nullable_text(value, "failure_reason", 128),
         {:ok, validated_at_ms} <- nullable_unsigned(value, "validated_at_ms") do
      {:ok,
       struct!(
         QueryIndexValidation,
         Map.merge(progress, %{
           status: status,
           checked_records: checked_records,
           checked_entries: checked_entries,
           mismatches: mismatches,
           failure_reason: failure_reason,
           validated_at_ms: validated_at_ms
         })
       )}
    end
  end

  def retirement(value) when is_map(value) do
    with {:ok, status} <-
           choice(value, "status", ~w(not_applicable pending complete), :retirement_status) do
      if status == "not_applicable" do
        {:ok,
         %QueryIndexRetirement{
           status: status,
           phase_counts: nil,
           current_phases: nil,
           completed_shards: nil,
           total_shards: nil,
           deleted_entries: nil,
           deleted_bytes: nil,
           rewritten_reverse_rows: nil,
           raw: value
         }}
      else
        retirement_progress(value, status)
      end
    end
  end

  def retirement(value), do: Validation.invalid(:retirement, value)

  defp retirement_progress(value, status) do
    with {:ok, phase_counts} <-
           IndexProgress.phase_counts(Types.get(value, "phase_counts"), :retirement),
         {:ok, current_phases} <-
           IndexProgress.phases(
             Types.get(value, "current_phases"),
             :retirement,
             @retirement_phases
           ),
         {:ok, completed_shards} <- Validation.unsigned(value, "completed_shards"),
         {:ok, total_shards} <- Validation.positive_unsigned(value, "total_shards"),
         true <- completed_shards <= total_shards,
         {:ok, deleted_entries} <- Validation.unsigned(value, "deleted_entries"),
         {:ok, deleted_bytes} <- Validation.unsigned(value, "deleted_bytes"),
         {:ok, rewritten_reverse_rows} <- Validation.unsigned(value, "rewritten_reverse_rows") do
      {:ok,
       %QueryIndexRetirement{
         status: status,
         phase_counts: phase_counts,
         current_phases: current_phases,
         completed_shards: completed_shards,
         total_shards: total_shards,
         deleted_entries: deleted_entries,
         deleted_bytes: deleted_bytes,
         rewritten_reverse_rows: rewritten_reverse_rows,
         raw: value
       }}
    else
      false -> Validation.invalid(:retirement, value)
      {:error, _reason} = error -> error
    end
  end

  defp nullable_text(value, field, maximum_bytes) do
    if Validation.has_key?(value, field) do
      case Types.get(value, field) do
        nil -> {:ok, nil}
        _present -> Validation.bounded_binary(value, field, maximum_bytes)
      end
    else
      Validation.invalid({:nullable, field}, value)
    end
  end

  defp nullable_unsigned(value, field) do
    if Validation.has_key?(value, field) do
      case Types.get(value, field) do
        nil -> {:ok, nil}
        _present -> Validation.unsigned(value, field)
      end
    else
      Validation.invalid({:nullable, field}, value)
    end
  end

  defp choice(value, field, choices, error_field) do
    actual = Types.get(value, field)
    if actual in choices, do: {:ok, actual}, else: Validation.invalid(error_field, actual)
  end
end
