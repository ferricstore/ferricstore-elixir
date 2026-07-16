defmodule FerricStore.Test.SlowJSON do
  @moduledoc false
  @enforce_keys [:owner]
  defstruct [:owner, delay: 100]
end

defimpl Jason.Encoder, for: FerricStore.Test.SlowJSON do
  def encode(%{owner: owner, delay: delay}, _opts) do
    send(owner, {:slow_json_encoder, self()})
    Process.sleep(delay)
    "null"
  end
end
