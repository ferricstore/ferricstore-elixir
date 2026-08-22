defmodule FerricStore.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {DynamicSupervisor, strategy: :one_for_one, name: FerricStore.HTTP.PoolSupervisor}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: FerricStore.Supervisor)
  end
end
