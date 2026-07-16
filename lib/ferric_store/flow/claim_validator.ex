defmodule FerricStore.Flow.ClaimValidator do
  @moduledoc false

  alias FerricStore.Types

  @max_exact 9_007_199_254_740_991

  @spec validate(map()) :: {:ok, map()} | {:error, :invalid_claim}
  def validate(claim) do
    id = Types.get(claim, :id)
    partition_key = Types.get(claim, :partition_key)
    lease_token = Types.get(claim, :lease_token)
    fencing_token = Types.get(claim, :fencing_token)
    run_state = Types.get(claim, :run_state)
    attributes = Types.get(claim, :attributes)

    if valid_identity?(id, partition_key, lease_token) and valid_fencing_token?(fencing_token) and
         valid_metadata?(run_state, attributes),
       do: {:ok, claim},
       else: {:error, :invalid_claim}
  end

  defp valid_identity?(id, partition_key, lease_token),
    do:
      nonempty_binary?(id) and optional_nonempty_binary?(partition_key) and
        nonempty_binary?(lease_token)

  defp valid_fencing_token?(value),
    do: is_integer(value) and value >= 0 and value <= @max_exact

  defp valid_metadata?(run_state, attributes),
    do: optional_nonempty_binary?(run_state) and (is_nil(attributes) or is_map(attributes))

  defp optional_nonempty_binary?(nil), do: true
  defp optional_nonempty_binary?(value), do: nonempty_binary?(value)
  defp nonempty_binary?(value), do: is_binary(value) and value != ""
end
