defmodule FerricStore.Transport.ResponseIdentity do
  @moduledoc false

  @type identity :: %{
          required(:request_id) => non_neg_integer(),
          required(:lane_id) => non_neg_integer(),
          required(:opcode) => non_neg_integer()
        }

  @spec validate(identity(), identity()) :: :ok | {:error, term()}
  def validate(expected, actual) do
    expected_identity = identity(expected)
    actual_identity = identity(actual)

    if expected_identity == actual_identity do
      :ok
    else
      {:error,
       {:protocol_response_mismatch, %{expected: expected_identity, actual: actual_identity}}}
    end
  end

  defp identity(value), do: {value.lane_id, value.opcode, value.request_id}
end
