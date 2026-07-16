defmodule FerricStore.ClientShutdown do
  @moduledoc false

  alias FerricStore.Timeout

  @spec stop(pid(), timeout()) :: :ok | {:error, {:close_failed, term()}}
  def stop(client, timeout) when is_pid(client) do
    if Timeout.valid?(timeout),
      do: stop_with_valid_timeout(client, timeout),
      else: {:error, {:close_failed, {:invalid_timeout, timeout}}}
  end

  def stop(_client, timeout), do: {:error, {:close_failed, {:invalid_timeout, timeout}}}

  defp stop_with_valid_timeout(client, timeout) do
    GenServer.stop(client, :normal, timeout)
  catch
    :exit, {:timeout, {GenServer, :stop, _arguments}} ->
      if Process.alive?(client), do: {:error, {:close_failed, :timeout}}, else: :ok

    :exit, reason ->
      if Process.alive?(client), do: {:error, {:close_failed, reason}}, else: :ok
  end
end
