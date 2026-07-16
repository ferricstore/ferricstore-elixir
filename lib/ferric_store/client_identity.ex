defmodule FerricStore.ClientIdentity do
  @moduledoc false

  @label_prefix __MODULE__
  @client_types [:topology_aware]

  @spec mark(:topology_aware, :ets.tid()) :: term()
  def mark(type, endpoint) when type in @client_types do
    Process.set_label({@label_prefix, type, endpoint})
  end

  @spec type(pid()) :: :topology_aware | :unknown | :dead
  def type(pid) when is_pid(pid) do
    case Process.info(pid, :label) do
      {:label, {@label_prefix, type, endpoint}} when type in @client_types ->
        if registered_client?(endpoint, pid), do: type, else: :unknown

      {:label, _other} ->
        :unknown

      nil ->
        :dead
    end
  end

  @spec endpoint(pid()) :: {:ok, :ets.tid()} | {:error, :unknown | :dead}
  def endpoint(pid) when is_pid(pid) do
    case Process.info(pid, :label) do
      {:label, {@label_prefix, type, endpoint}} when type in @client_types ->
        if registered_client?(endpoint, pid), do: {:ok, endpoint}, else: {:error, :unknown}

      {:label, _other} ->
        {:error, :unknown}

      nil ->
        {:error, :dead}
    end
  end

  defp registered_client?(endpoint, pid) do
    :ets.lookup(endpoint, :client) == [{:client, pid}]
  rescue
    ArgumentError -> false
  end
end
