defmodule FerricStore.SDK.Native.CoordinatorInitializer do
  @moduledoc false

  alias FerricStore.SDK.Native.Coordinator.State
  alias FerricStore.SDK.Native.EndpointPolicy

  @spec run(keyword(), (State.t() -> {:ok, State.t()} | {:error, term()})) ::
          {:ok, State.t()} | {:stop, term()}
  def run(opts, refresh_topology) when is_list(opts) and is_function(refresh_topology, 1) do
    endpoint_options = EndpointPolicy.options(opts)

    case EndpointPolicy.normalize_seeds(
           Keyword.fetch!(opts, :seeds),
           Keyword.get(opts, :tls, false),
           endpoint_options
         ) do
      {:ok, seeds} ->
        initialize(opts, seeds, endpoint_options, refresh_topology)

      {:error, reason} ->
        {:stop, reason}
    end
  end

  defp initialize(opts, seeds, endpoint_options, refresh_topology) do
    state = State.new(opts, seeds, endpoint_options)

    case refresh_topology.(state) do
      {:ok, state} -> {:ok, state}
      {:error, reason} -> {:stop, reason}
    end
  end
end
