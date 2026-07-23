defmodule FerricStore.Flow.QueryResponse.Explain do
  @moduledoc false

  alias FerricStore.Flow.QueryExplainResult
  alias FerricStore.Flow.QueryResponse.{Diagnostic, Validation}
  alias FerricStore.Types

  @contract "ferric.flow.explain/v1"

  def decode(value) when is_map(value) do
    with {:ok, @contract} <- Validation.contract(value, "version", @contract),
         {:ok, fingerprint} <- Validation.query_fingerprint(value, "query_fingerprint"),
         {:ok, status} <- status(value),
         {:ok, plan} <- Validation.required_map(value, "plan"),
         {:ok, estimate} <- Validation.required_map(value, "estimate"),
         {:ok, bounds} <- Validation.required_map(value, "bounds"),
         {:ok, actual} <- actual(value, status),
         {:ok, diagnostic} <- diagnostic(value, status) do
      {:ok,
       %QueryExplainResult{
         version: @contract,
         query_fingerprint: fingerprint,
         status: status,
         plan: plan,
         estimate: estimate,
         bounds: bounds,
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
