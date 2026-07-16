defmodule FerricStore.Transport.SessionPolicy do
  @moduledoc false

  alias FerricStore.Protocol.{CommandSpec, PreparedMap}

  @goaway_opcode CommandSpec.fetch!(:goaway).opcode
  @pipeline_opcode CommandSpec.fetch!(:pipeline).opcode
  @first_data_opcode CommandSpec.fetch!(:command_exec).opcode
  @max_request_id 0xFFFF_FFFF_FFFF_FFFF

  @type server_frame_action :: :continue | :drain

  @spec server_frame_action(non_neg_integer()) :: server_frame_action()
  def server_frame_action(@goaway_opcode), do: :drain
  def server_frame_action(_opcode), do: :continue

  @spec put_deadline(term(), non_neg_integer(), timeout()) :: term()
  def put_deadline(%PreparedMap{} = payload, opcode, timeout)
      when is_integer(timeout) and timeout >= 0 and
             (opcode >= @first_data_opcode or opcode == @pipeline_opcode) do
    PreparedMap.put_reserved(
      payload,
      "deadline_ms",
      System.system_time(:millisecond) + timeout
    )
  end

  def put_deadline(payload, opcode, timeout)
      when is_map(payload) and is_integer(timeout) and timeout >= 0 and
             (opcode >= @first_data_opcode or opcode == @pipeline_opcode) do
    payload
    |> Map.delete(:deadline_ms)
    |> Map.put("deadline_ms", System.system_time(:millisecond) + timeout)
  end

  def put_deadline(payload, _opcode, _timeout), do: payload

  @spec next_request_id(non_neg_integer()) :: pos_integer()
  def next_request_id(@max_request_id), do: 1
  def next_request_id(request_id), do: request_id + 1

  @spec available_request_id(pos_integer(), map()) :: pos_integer()
  def available_request_id(request_id, pending) do
    if Map.has_key?(pending, request_id) do
      available_request_id(next_request_id(request_id), pending)
    else
      request_id
    end
  end
end
