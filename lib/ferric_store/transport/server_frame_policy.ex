defmodule FerricStore.Transport.ServerFramePolicy do
  @moduledoc false

  alias FerricStore.Protocol.CommandSpec

  @event_opcode CommandSpec.fetch!(:event).opcode
  @goaway_opcode CommandSpec.fetch!(:goaway).opcode

  @type kind :: :connection_error | :management

  @spec classify(non_neg_integer(), non_neg_integer()) ::
          {:ok, kind()} | {:error, {:invalid_server_frame, map()}}
  def classify(0, 0), do: {:ok, :connection_error}
  def classify(0, opcode) when opcode in [@event_opcode, @goaway_opcode], do: {:ok, :management}

  def classify(lane_id, opcode) do
    {:error, {:invalid_server_frame, %{lane_id: lane_id, opcode: opcode}}}
  end

  @spec resolve(kind(), {:ok, term(), term()} | {:error, term()}) ::
          {:deliver, term()} | {:stop, term()}
  def resolve(:management, {:ok, :ok, value}), do: {:deliver, value}

  def resolve(:management, {:ok, status, _value}),
    do: {:stop, {:invalid_server_frame_status, status}}

  def resolve(:connection_error, {:ok, :ok, _value}),
    do: {:stop, {:invalid_server_frame_status, :ok}}

  def resolve(:connection_error, {:ok, status, value}),
    do: {:stop, {:server_error, status, value}}

  def resolve(_kind, {:error, reason}),
    do: {:stop, {:invalid_server_frame_payload, reason}}
end
