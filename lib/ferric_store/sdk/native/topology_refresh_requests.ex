defmodule FerricStore.SDK.Native.TopologyRefreshRequests do
  @moduledoc false

  alias FerricStore.{RequestContext, Timeout}
  alias FerricStore.SDK.Native.CoordinatorCall

  @default_timeout 5_000

  @spec submit(pid(), timeout()) :: :ok | {:error, term()}
  def submit(client, timeout) do
    if Timeout.valid?(timeout) do
      context = RequestContext.new([timeout: timeout], @default_timeout)

      with :ok <- RequestContext.ensure_active(context) do
        CoordinatorCall.submit(
          client,
          {:refresh_topology, context},
          RequestContext.remaining(context)
        )
      end
    else
      {:error, {:invalid_timeout, timeout}}
    end
  end
end
