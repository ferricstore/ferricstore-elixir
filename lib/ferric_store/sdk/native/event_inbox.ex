defmodule FerricStore.SDK.Native.EventInbox do
  @moduledoc false

  alias FerricStore.{ClientIdentity, Timeout}
  alias FerricStore.SDK.Native.EventTerminal

  @type result ::
          {:ok, map()}
          | {:error, :client_closed | {:client_unavailable, :invalid_client}}
          | {:error, {:invalid_timeout, term()}}
          | nil

  @spec await(term(), term()) :: result()
  def await(client, timeout) when is_pid(client) do
    if Timeout.valid?(timeout) do
      case ClientIdentity.type(client) do
        :topology_aware -> EventTerminal.await(client, timeout)
        :unknown -> invalid_client()
        :dead -> {:error, :client_closed}
      end
    else
      {:error, {:invalid_timeout, timeout}}
    end
  end

  def await(_client, _timeout), do: invalid_client()

  defp invalid_client, do: {:error, {:client_unavailable, :invalid_client}}
end
