defmodule FerricStore.SDK.Native.CoordinatorRetryTarget do
  @moduledoc false

  alias FerricStore.RequestContext
  alias FerricStore.SDK.Native.{Topology, TopologyRuntime}

  @spec control(map(), term()) :: {map(), term()}
  def control(state, opts) do
    case RequestContext.option(opts, :endpoint) do
      nil ->
        endpoint = TopologyRuntime.control_endpoint(state)
        {endpoint, Topology.endpoint_key(endpoint)}

      endpoint ->
        {endpoint, nil}
    end
  end
end
