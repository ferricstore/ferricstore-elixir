defmodule FerricStore.Flow.ClaimNormalizer do
  @moduledoc false

  alias FerricStore.Flow.ClaimValidator

  @spec normalize(term()) :: {:ok, map()} | {:error, :invalid_claim}
  def normalize([id, partition_key, lease_token, fencing_token]) do
    ClaimValidator.validate(%{
      "id" => id,
      "partition_key" => partition_key,
      "lease_token" => lease_token,
      "fencing_token" => fencing_token
    })
  end

  def normalize([id, partition_key, lease_token, fencing_token, attributes])
      when is_map(attributes) do
    ClaimValidator.validate(%{
      "id" => id,
      "partition_key" => partition_key,
      "lease_token" => lease_token,
      "fencing_token" => fencing_token,
      "attributes" => attributes
    })
  end

  def normalize([id, partition_key, lease_token, fencing_token, run_state]) do
    ClaimValidator.validate(%{
      "id" => id,
      "partition_key" => partition_key,
      "lease_token" => lease_token,
      "fencing_token" => fencing_token,
      "run_state" => run_state
    })
  end

  def normalize([id, partition_key, lease_token, fencing_token, run_state, attributes]) do
    ClaimValidator.validate(%{
      "id" => id,
      "partition_key" => partition_key,
      "lease_token" => lease_token,
      "fencing_token" => fencing_token,
      "run_state" => run_state,
      "attributes" => attributes
    })
  end

  def normalize(%{} = claim), do: ClaimValidator.validate(claim)
  def normalize(_claim), do: {:error, :invalid_claim}
end
