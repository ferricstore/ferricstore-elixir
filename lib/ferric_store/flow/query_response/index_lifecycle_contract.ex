defmodule FerricStore.Flow.QueryResponse.IndexLifecycleContract do
  @moduledoc false

  alias FerricStore.Flow.QueryResponse.Validation, as: V

  @build_phases ~w(pending snapshot backfill done)
  @validation_phases ~w(pending source index counter cleanup done)
  @retirement_phases ~w(pending fence index counter reverse cleanup done)
  @retirement_progress ~w(
                         phase_counts current_phases completed_shards total_shards
                         deleted_entries deleted_bytes rewritten_reverse_rows
                       )

  def validate(index) do
    with :ok <- validate_progress(index.build, @build_phases, :build),
         :ok <- validate_progress(index.validation, @validation_phases, :validation),
         :ok <- validate_retirement(index.retirement),
         :ok <- validate_shards(index),
         :ok <- validate_lifecycle(index),
         do: validate_validation(index.validation)
  end

  defp validate_progress(progress, phases, section) do
    counts = progress.phase_counts
    expected_current = Enum.filter(phases, &Map.has_key?(counts, &1))

    if map_size(counts) > 0 and
         Enum.all?(counts, fn {phase, count} -> phase in phases and count > 0 end) and
         Enum.sum(Map.values(counts)) == progress.total_shards and
         progress.current_phases == expected_current and
         progress.completed_shards == Map.get(counts, "done", 0),
       do: :ok,
       else: V.invalid({section, :progress}, progress.raw)
  end

  defp validate_retirement(%{status: "not_applicable"} = retirement) do
    if Enum.any?(@retirement_progress, &V.has_key?(retirement.raw, &1)),
      do: V.invalid(:retirement_progress, retirement.raw),
      else: :ok
  end

  defp validate_retirement(retirement),
    do: validate_progress(retirement, @retirement_phases, :retirement)

  defp validate_shards(index) do
    totals = [
      index.coverage.total_shards,
      index.build.total_shards,
      index.validation.total_shards
    ]

    totals =
      if index.retirement.total_shards == nil,
        do: totals,
        else: [index.retirement.total_shards | totals]

    queryable =
      index.state == "active" and
        index.coverage.complete_shards == index.coverage.total_shards and
        index.coverage.validation == "passed"

    if length(Enum.uniq(totals)) == 1 and
         index.coverage.complete_shards == index.build.completed_shards and
         index.coverage.validation == index.validation.status and index.queryable == queryable,
       do: :ok,
       else: V.invalid(:index_shards, index.raw)
  end

  defp validate_lifecycle(index) do
    built = index.build.completed_shards == index.build.total_shards

    valid =
      valid_lifecycle?(
        index.state,
        built,
        index.validation.status,
        index.retirement.status
      )

    if valid, do: :ok, else: V.invalid(:index_lifecycle, index.raw)
  end

  defp valid_lifecycle?("building", false, "pending", "not_applicable"), do: true

  defp valid_lifecycle?("validating", true, validation, "not_applicable")
       when validation in ~w(pending passed),
       do: true

  defp valid_lifecycle?("active", true, "passed", "not_applicable"), do: true

  defp valid_lifecycle?("retiring", true, validation, retirement)
       when validation in ~w(passed failed) and retirement in ~w(pending complete),
       do: true

  defp valid_lifecycle?("failed", _built, validation, retirement)
       when validation in ~w(passed failed) and retirement in ~w(pending complete),
       do: true

  defp valid_lifecycle?(_state, _built, _validation, _retirement), do: false

  defp validate_validation(validation) do
    if valid_validation?(validation),
      do: :ok,
      else: V.invalid(:index_validation, validation.raw)
  end

  defp valid_validation?(%{
         status: "pending",
         mismatches: 0,
         failure_reason: nil,
         validated_at_ms: nil
       }),
       do: true

  defp valid_validation?(%{
         status: "passed",
         mismatches: 0,
         failure_reason: nil,
         validated_at_ms: validated_at_ms
       })
       when not is_nil(validated_at_ms),
       do: true

  defp valid_validation?(%{
         status: "failed",
         mismatches: mismatches,
         failure_reason: failure_reason,
         validated_at_ms: validated_at_ms
       })
       when mismatches > 0 and is_binary(failure_reason) and failure_reason != "" and
              not is_nil(validated_at_ms),
       do: true

  defp valid_validation?(_validation), do: false
end
