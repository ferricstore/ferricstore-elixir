defmodule FerricStore.HTTP.PoolSupervisor do
  @moduledoc false

  @http1 FerricStore.HTTP.Finch.HTTP1
  @http2 FerricStore.HTTP.Finch.HTTP2

  @spec ensure_started(boolean()) :: {:ok, atom()} | {:error, term()}
  def ensure_started(http2) when is_boolean(http2) do
    name = if http2, do: @http2, else: @http1

    case Process.whereis(name) do
      pid when is_pid(pid) -> {:ok, name}
      nil -> start_pool(name, http2)
    end
  end

  defp start_pool(name, http2) do
    options = [name: name, pools: %{default: pool_options(http2)}]

    case DynamicSupervisor.start_child(__MODULE__, {Finch, options}) do
      {:ok, _pid} -> {:ok, name}
      {:error, {:already_started, _pid}} -> {:ok, name}
      {:error, reason} -> {:error, {:http_pool_start_failed, reason}}
    end
  end

  defp pool_options(false) do
    [
      protocols: [:http1],
      count: 1,
      size: 100,
      conn_max_idle_time: 90_000,
      conn_opts: [transport_opts: transport_options()]
    ]
  end

  defp pool_options(true) do
    [
      protocols: [:http2],
      count: 1,
      http2: [wait_for_server_settings?: true],
      conn_opts: [transport_opts: transport_options()]
    ]
  end

  defp transport_options,
    do: Application.get_env(:ferricstore_sdk, :http_pool_transport_options, [])
end
