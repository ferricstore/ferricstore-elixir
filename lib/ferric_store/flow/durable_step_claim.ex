defmodule FerricStore.Flow.DurableStepClaim do
  @moduledoc false

  alias FerricStore.Flow.ClaimNormalizer
  alias FerricStore.{Result, Types}

  @claim_fields ~w(id partition_key lease_token fencing_token run_state type state payload attributes)

  @spec validate(term(), :advance | :step) :: {:ok, map()} | {:error, term()}
  def validate(job, operation) do
    with {:ok, job} <- ClaimNormalizer.normalize(job),
         run_state when is_binary(run_state) and run_state != "" <- Types.get(job, :run_state),
         fencing_token when is_integer(fencing_token) and fencing_token > 0 <-
           Types.get(job, :fencing_token),
         true <- valid_active_state?(job) do
      {:ok, job}
    else
      _invalid -> {:error, {:invalid_flow_claim, operation, :expected_claimed_job}}
    end
  end

  @spec credentials(map()) :: map()
  def credentials(job) do
    %{
      "id" => Types.get(job, :id),
      "partition_key" => Types.get(job, :partition_key),
      "lease_token" => Types.get(job, :lease_token),
      "fencing_token" => Types.get(job, :fencing_token)
    }
  end

  @spec validate_extended(map(), term()) :: {:ok, map()} | {:error, term()}
  def validate_extended(job, response) do
    with {:ok, record} <- ClaimNormalizer.normalize(response),
         true <- same_claim?(job, record),
         "running" <- Types.get(record, :state),
         true <- same_physical_state?(job, record),
         true <- Types.get(record, :run_state) == Types.get(job, :run_state),
         true <- valid_value_refs?(record) do
      {:ok, record}
    else
      _invalid -> invalid_response(:invalid_extended_claim)
    end
  end

  @spec normalize_refreshed(map(), term(), binary()) :: {:ok, map()} | {:error, term()}
  def normalize_refreshed(job, response, run_state) do
    with {:ok, claim} <- ClaimNormalizer.normalize(response),
         true <- same_identity?(job, claim),
         true <- changed_lease?(job, claim),
         true <- increased_fence?(job, claim),
         true <- expected_state?(response, claim),
         true <- expected_run_state?(response, claim, run_state) do
      {:ok,
       job
       |> refresh(claim)
       |> Map.put("state", "running")
       |> Map.put("run_state", run_state)}
    else
      _invalid -> invalid_response(:invalid_refreshed_claim)
    end
  end

  @spec refresh(map(), map()) :: map()
  def refresh(job, refreshed) do
    Map.merge(select_fields(job), select_fields(refreshed))
  end

  defp select_fields(map) do
    Enum.reduce(@claim_fields, %{}, fn field, selected ->
      case fetch_field(map, field) do
        {:ok, value} -> Map.put(selected, field, value)
        :error -> selected
      end
    end)
  end

  defp fetch_field(map, field) do
    case Map.fetch(map, field) do
      {:ok, value} -> {:ok, value}
      :error -> fetch_atom_field(map, field)
    end
  end

  defp fetch_atom_field(map, field) do
    Map.fetch(map, String.to_existing_atom(field))
  rescue
    ArgumentError -> :error
  end

  defp same_claim?(job, claim) do
    same_identity?(job, claim) and Types.get(job, :lease_token) == Types.get(claim, :lease_token) and
      Types.get(job, :fencing_token) == Types.get(claim, :fencing_token)
  end

  defp same_identity?(job, claim),
    do:
      Types.get(job, :id) == Types.get(claim, :id) and
        Types.get(job, :partition_key) == Types.get(claim, :partition_key)

  defp valid_active_state?(job) do
    case fetch_field(job, "state") do
      :error -> true
      {:ok, "running"} -> true
      {:ok, _invalid} -> false
    end
  end

  defp same_physical_state?(job, claim) do
    case fetch_field(job, "state") do
      :error -> Types.get(claim, :state) == "running"
      {:ok, state} -> Types.get(claim, :state) == state
    end
  end

  defp changed_lease?(job, claim),
    do: Types.get(job, :lease_token) != Types.get(claim, :lease_token)

  defp increased_fence?(job, claim),
    do: Types.get(claim, :fencing_token) > Types.get(job, :fencing_token)

  defp expected_run_state?(response, claim, run_state) when is_list(response),
    do: Types.get(claim, :run_state) in [nil, run_state]

  defp expected_run_state?(_response, claim, run_state),
    do: Types.get(claim, :run_state) == run_state

  defp expected_state?(response, claim) when is_list(response),
    do: Types.get(claim, :state) in [nil, "running"]

  defp expected_state?(_response, claim), do: Types.get(claim, :state) == "running"

  defp valid_value_refs?(record) do
    case fetch_field(record, "value_refs") do
      :error -> true
      {:ok, value_refs} -> is_map(value_refs)
    end
  end

  defp invalid_response(reason),
    do: Result.error({:invalid_flow_response, %{operation: :step, reason: reason}})
end
