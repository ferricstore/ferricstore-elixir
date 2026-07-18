defmodule FerricStore.SDK.Native.ConnectionEncoder do
  @moduledoc false

  alias FerricStore.Protocol.CommandSpec

  alias FerricStore.SDK.Native.ConnectionEncodingWorker

  @enforce_keys [:control, :data]
  defstruct [:control, :data, compact_response_codecs: %{}]

  @type t :: %__MODULE__{
          control: pid(),
          data: pid(),
          compact_response_codecs: %{optional(non_neg_integer()) => binary()}
        }

  @spec start(pid()) :: t()
  def start(owner) when is_pid(owner) do
    %__MODULE__{
      control: ConnectionEncodingWorker.start(owner),
      data: ConnectionEncodingWorker.start(owner)
    }
  end

  @spec enqueue(
          map(),
          non_neg_integer(),
          reference(),
          non_neg_integer(),
          term(),
          non_neg_integer(),
          timeout(),
          integer() | :infinity
        ) :: :ok
  def enqueue(state, request_id, encode_token, opcode, payload, lane_id, timeout, deadline) do
    job = %{
      request_id: request_id,
      encode_token: encode_token,
      opcode: opcode,
      payload: payload,
      lane_id: lane_id,
      timeout: timeout,
      deadline: deadline,
      max_pipeline_commands: state.max_pipeline_commands,
      max_request_bytes: state.max_request_bytes,
      compact_response_codec: Map.get(state.encoder.compact_response_codecs, opcode),
      transport: state.transport,
      socket: state.socket
    }

    encoder = state.encoder
    worker = if CommandSpec.control_lane?(opcode), do: encoder.control, else: encoder.data
    send(worker, {:encode, job})
    :ok
  end

  @spec worker?(t(), pid()) :: boolean()
  def worker?(%__MODULE__{control: worker}, worker), do: true
  def worker?(%__MODULE__{data: worker}, worker), do: true
  def worker?(%__MODULE__{}, _worker), do: false

  @spec put_response_codecs(t(), map()) :: t()
  def put_response_codecs(%__MODULE__{} = encoder, codecs) when is_map(codecs),
    do: %{encoder | compact_response_codecs: codecs}

  @spec authorize_send(pid(), non_neg_integer(), reference()) :: :ok
  def authorize_send(worker, request_id, encode_token)
      when is_pid(worker) and is_integer(request_id) and is_reference(encode_token) do
    send(worker, {:authorize_send, request_id, encode_token})
    :ok
  end

  @spec discard(pid(), non_neg_integer(), reference()) :: :ok
  def discard(worker, request_id, encode_token)
      when is_pid(worker) and is_integer(request_id) and is_reference(encode_token) do
    send(worker, {:discard, request_id, encode_token})
    :ok
  end

  @spec stop(t() | nil) :: :ok
  def stop(nil), do: :ok

  def stop(%__MODULE__{} = encoder) do
    stop_worker(encoder.control)
    stop_worker(encoder.data)
    :ok
  end

  defp stop_worker(worker) do
    Process.unlink(worker)
    Process.exit(worker, :kill)
  end
end
