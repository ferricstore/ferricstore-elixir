defmodule FerricStore.SDK.Native.KVPayloadPreparer do
  @moduledoc false

  alias FerricStore.{FailureFormatter, RequestContext}
  alias FerricStore.Protocol.{PreparedMap, PreparedMSet}
  alias FerricStore.SDK.Native.ConnectionOptions

  @type operation :: :del | :mget | :mset
  @type prepare_error ::
          :request_too_large
          | :timeout
          | {:encode_failed, binary()}
          | {:invalid_prepared_payload, operation()}

  @spec prepare(map(), operation(), RequestContext.t()) ::
          {:ok, map()} | {:error, prepare_error()}
  def prepare(
        %{route: %{endpoint: endpoint}, items: items, payload: payload} = group,
        operation,
        %RequestContext{} = context
      )
      when operation in [:del, :mget, :mset] and is_list(items) and is_map(payload) do
    max_request_bytes = ConnectionOptions.max_request_bytes(endpoint)
    reservations = deadline_reservations(context)

    with :ok <- RequestContext.ensure_active(context),
         {:ok, prepared} <-
           prepare_payload(operation, items, payload, max_request_bytes, reservations),
         :ok <- RequestContext.ensure_active(context) do
      prepared =
        PreparedMap.put_metadata(prepared, %{operation: operation, items: items})

      {:ok, group |> Map.delete(:items) |> Map.put(:payload, prepared)}
    else
      {:error, :too_large} -> {:error, :request_too_large}
      {:error, :invalid_pairs} -> {:error, {:invalid_prepared_payload, :mset}}
      {:error, :timeout} = error -> error
      {:error, _reason} = error -> error
    end
  rescue
    error in ArgumentError ->
      {:error,
       {:encode_failed, FailureFormatter.exception_message(error, "payload encoding failed")}}
  end

  defp prepare_payload(
         :mset,
         items,
         %{"pairs" => pairs} = payload,
         max_request_bytes,
         reservations
       )
       when map_size(payload) == 1 and pairs === items,
       do: PreparedMSet.prepare(items, max_request_bytes, reservations)

  defp prepare_payload(:mset, _items, _payload, _max_request_bytes, _reservations),
    do: {:error, {:invalid_prepared_payload, :mset}}

  defp prepare_payload(
         operation,
         items,
         %{"keys" => keys} = payload,
         max_request_bytes,
         reservations
       )
       when operation in [:del, :mget] and map_size(payload) == 1 and keys === items,
       do: PreparedMap.prepare(payload, max_request_bytes, reservations)

  defp prepare_payload(operation, _items, _payload, _max_request_bytes, _reservations)
       when operation in [:del, :mget],
       do: {:error, {:invalid_prepared_payload, operation}}

  defp deadline_reservations(context) do
    case RequestContext.remaining(context) do
      :infinity -> []
      _finite -> [{"deadline_ms", 0}]
    end
  end
end
