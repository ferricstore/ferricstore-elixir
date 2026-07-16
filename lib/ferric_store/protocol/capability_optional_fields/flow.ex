defmodule FerricStore.Protocol.CapabilityOptionalFields.Flow do
  @moduledoc false

  alias FerricStore.Protocol.CapabilityOptionalFields.{
    FlowLifecycle,
    FlowOrchestration,
    FlowPolicy,
    FlowQueries,
    FlowSchedules,
    FlowValues
  }

  @spec all() :: %{binary() => [binary()]}
  def all do
    FlowLifecycle.all()
    |> Map.merge(FlowOrchestration.all())
    |> Map.merge(FlowQueries.all())
    |> Map.merge(FlowPolicy.all())
    |> Map.merge(FlowSchedules.all())
    |> Map.merge(FlowValues.all())
  end
end
