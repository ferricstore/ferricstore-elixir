defmodule FerricStore.Flow.QueryResponse.Explain do
  @moduledoc false

  alias FerricStore.Flow.{QueryExplainCapabilities, QueryExplainResult}
  alias FerricStore.Flow.QueryResponse.{Diagnostic, Validation}
  alias FerricStore.Types

  @contract "ferric.flow.explain/v1"
  @extended_fields ~w(stats quality pressure decision alternatives)

  def decode(value) when is_map(value) do
    with {:ok, @contract} <- Validation.contract(value, "version", @contract),
         {:ok, fingerprint} <- Validation.query_fingerprint(value, "query_fingerprint"),
         {:ok, status} <- status(value),
         {:ok, plan} <- Validation.required_map(value, "plan"),
         {:ok, estimate} <- Validation.required_map(value, "estimate"),
         {:ok, bounds} <- Validation.required_map(value, "bounds"),
         {:ok, capabilities} <- capabilities(value),
         {:ok, extended} <- extended_envelope(value, status, capabilities),
         {:ok, actual} <- actual(value, status),
         {:ok, diagnostic} <- diagnostic(value, status) do
      {:ok,
       %QueryExplainResult{
         version: @contract,
         query_fingerprint: fingerprint,
         status: status,
         plan: plan,
         estimate: estimate,
         capabilities: capabilities,
         stats: extended.stats,
         quality: extended.quality,
         bounds: bounds,
         pressure: extended.pressure,
         decision: extended.decision,
         alternatives: extended.alternatives,
         actual: actual,
         diagnostic: diagnostic,
         raw: value
       }}
    end
  end

  def decode(value), do: Validation.invalid(:explain, value)

  defp status(value) do
    case Types.get(value, "status") do
      status when status in ["planned", "rejected", "executed"] -> {:ok, status}
      status -> Validation.invalid(:explain_status, status)
    end
  end

  defp alternatives(value) when is_list(value) and length(value) <= 31 do
    if Enum.all?(value, &is_map/1),
      do: {:ok, value},
      else: Validation.invalid(:explain_alternatives, value)
  end

  defp alternatives(value), do: Validation.invalid(:explain_alternatives, value)

  defp capabilities(value) do
    if Validation.has_key?(value, "capabilities") do
      with {:ok, raw} <- Validation.required_map(value, "capabilities"),
           {:ok, requested} <- capability_list(raw, "requested"),
           {:ok, available} <- capability_list(raw, "available"),
           {:ok, missing} <- capability_list(raw, "missing") do
        {:ok,
         %QueryExplainCapabilities{
           requested: requested,
           available: available,
           missing: missing,
           raw: raw
         }}
      end
    else
      {:ok, nil}
    end
  end

  defp capability_list(value, field) do
    case Types.get(value, field) do
      items when is_list(items) and length(items) <= 64 ->
        decode_capability_items(items, field)

      invalid ->
        Validation.invalid({:explain_capabilities, field}, invalid)
    end
  end

  defp decode_capability_items(items, field) do
    items
    |> Enum.reduce_while({:ok, [], MapSet.new()}, fn item, {:ok, decoded, seen} ->
      if is_binary(item) and item != "" and String.valid?(item) and byte_size(item) <= 128 and
           not MapSet.member?(seen, item) do
        {:cont, {:ok, [item | decoded], MapSet.put(seen, item)}}
      else
        {:halt, Validation.invalid({:explain_capabilities, field}, items)}
      end
    end)
    |> case do
      {:ok, decoded, _seen} -> {:ok, Enum.reverse(decoded)}
      {:error, _reason} = error -> error
    end
  end

  defp extended_envelope(value, status, capabilities) do
    present = Enum.map(@extended_fields, &Validation.has_key?(value, &1))

    if capabilities != nil and not Enum.any?(present) do
      specialized_envelope(value, status)
    else
      actionable_envelope(value, present)
    end
  end

  defp specialized_envelope(value, "planned") do
    if Validation.has_key?(value, "actual") or Validation.has_key?(value, "diagnostic") do
      Validation.invalid(:explain_specialized_status_fields, value)
    else
      {:ok, %{stats: nil, quality: nil, pressure: nil, decision: nil, alternatives: []}}
    end
  end

  defp specialized_envelope(value, _status),
    do: Validation.invalid(:explain_specialized_status, value)

  defp actionable_envelope(value, present) do
    with true <- Enum.all?(present),
         :ok <- require_nullable_fields(value),
         {:ok, stats} <- Validation.required_map(value, "stats"),
         {:ok, quality} <- Validation.quality(Types.get(value, "quality")),
         {:ok, pressure} <- Validation.required_map(value, "pressure"),
         {:ok, decision} <- Validation.required_map(value, "decision"),
         {:ok, alternatives} <- alternatives(Types.get(value, "alternatives")) do
      {:ok,
       %{
         stats: stats,
         quality: quality,
         pressure: pressure,
         decision: decision,
         alternatives: alternatives
       }}
    else
      false -> Validation.invalid(:explain_required_fields, value)
      {:error, _reason} = error -> error
    end
  end

  defp require_nullable_fields(value) do
    if Validation.has_key?(value, "actual") and Validation.has_key?(value, "diagnostic"),
      do: :ok,
      else: Validation.invalid(:explain_nullable_fields, value)
  end

  defp actual(value, "executed") do
    case Types.get(value, "actual") do
      nil -> Validation.invalid(:explain_actual, nil)
      usage -> Validation.usage(usage)
    end
  end

  defp actual(value, _status) do
    case Types.get(value, "actual") do
      nil -> {:ok, nil}
      actual -> Validation.invalid(:explain_actual, actual)
    end
  end

  defp diagnostic(value, "rejected") do
    case Types.get(value, "diagnostic") do
      nil -> Validation.invalid(:explain_diagnostic, nil)
      diagnostic -> Diagnostic.decode(diagnostic, diagnostic)
    end
  end

  defp diagnostic(value, _status) do
    case Types.get(value, "diagnostic") do
      nil -> {:ok, nil}
      diagnostic -> Validation.invalid(:explain_diagnostic, diagnostic)
    end
  end
end
