defmodule FerricStore.Protocol.CapabilityOptionalFields.FlowPolicy do
  @moduledoc false

  @fields %{
    "FLOW.POLICY.SET" => [
      "expected_generation",
      "replace",
      "max_active_ms",
      "retry",
      "retention",
      "states",
      "indexed_attributes",
      "indexed_state_meta",
      "version",
      "governance"
    ],
    "FLOW.POLICY.GET" => ["state"]
  }

  @spec all() :: map()
  def all, do: @fields
end
