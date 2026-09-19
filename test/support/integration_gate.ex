defmodule FerricStore.Test.IntegrationGate do
  @moduledoc false

  def rewind_reason_skip("0.11.4"),
    do: "FerricStore 0.11.4 does not persist FLOW.REWIND binary reasons"

  def rewind_reason_skip(_server_version), do: false
end
