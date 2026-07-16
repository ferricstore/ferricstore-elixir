defmodule FerricStore.Transport.RequestEncoder do
  @moduledoc false

  alias FerricStore.{FailureFormatter, Protocol}

  @spec encode(non_neg_integer(), non_neg_integer(), non_neg_integer(), term(), pos_integer()) ::
          {:ok, iodata()} | {:error, term()}
  def encode(opcode, lane_id, request_id, payload, max_request_bytes) do
    {:ok,
     Protocol.encode_request_iodata(opcode, request_id, Protocol.payload_or_empty(payload),
       lane_id: lane_id,
       max_body_bytes: max_request_bytes
     )}
  rescue
    Protocol.RequestTooLargeError ->
      {:error, :request_too_large}

    error ->
      {:error,
       {:encode_failed, FailureFormatter.exception_message(error, "request encoding failed")}}
  catch
    kind, reason -> {:error, {:encode_failed, FailureFormatter.inspect_term({kind, reason})}}
  end
end
