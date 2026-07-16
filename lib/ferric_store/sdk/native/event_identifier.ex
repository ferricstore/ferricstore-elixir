defmodule FerricStore.SDK.Native.EventIdentifier do
  @moduledoc false

  @supported_events [
    "AUTH_INVALIDATED",
    "BACKPRESSURE_CHANGED",
    "FLOW_WAKE",
    "GOAWAY",
    "TOPOLOGY_CHANGED"
  ]
  @supported_event_set MapSet.new(@supported_events)
  @max_bytes 128

  @spec supported() :: [binary()]
  def supported, do: @supported_events

  @spec normalize(term()) :: {:ok, binary()} | {:error, atom()}
  def normalize(event) when is_binary(event) do
    cond do
      event == "" -> {:error, :expected_nonempty_binary_or_atom}
      byte_size(event) > @max_bytes -> {:error, :identifier_too_long}
      not String.valid?(event) -> {:error, :invalid_utf8}
      true -> supported_identifier(String.upcase(event))
    end
  end

  def normalize(event) when is_atom(event) and event not in [nil, false, true, :""],
    do: event |> Atom.to_string() |> normalize()

  def normalize(_event), do: {:error, :expected_nonempty_binary_or_atom}

  defp supported_identifier(event) do
    if byte_size(event) <= @max_bytes and MapSet.member?(@supported_event_set, event),
      do: {:ok, event},
      else: {:error, :unsupported_event}
  end
end
