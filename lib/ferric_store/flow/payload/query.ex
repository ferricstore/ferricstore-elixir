defmodule FerricStore.Flow.Payload.Query do
  @moduledoc false

  import FerricStore.Flow.Payload.Normalize,
    only: [put_if_present: 3, stringify_map: 1]

  alias FerricStore.Flow.Payload.Policy

  def get_payload(id, opts) do
    %{"id" => id}
    |> put_if_present("partition_key", Keyword.get(opts, :partition_key))
    |> put_if_present("full", Keyword.get(opts, :full))
    |> put_if_present("payload", Keyword.get(opts, :payload))
    |> put_if_present("payload_max_bytes", Keyword.get(opts, :payload_max_bytes))
    |> put_if_present("values", Keyword.get(opts, :values))
  end

  def list_payload(opts) do
    %{"type" => Keyword.fetch!(opts, :type)}
    |> put_if_present("state", Keyword.get(opts, :state))
    |> put_if_present("partition_key", Keyword.get(opts, :partition_key))
    |> put_if_present("count", Keyword.get(opts, :count))
    |> put_if_present("from_ms", Keyword.get(opts, :from_ms))
    |> put_if_present("to_ms", Keyword.get(opts, :to_ms))
    |> put_if_present("rev", Keyword.get(opts, :rev))
    |> put_if_present("attributes", stringify_map(Keyword.get(opts, :attributes)))
    |> put_if_present("include_cold", Keyword.get(opts, :include_cold))
    |> put_if_present("consistent_projection", Keyword.get(opts, :consistent_projection))
    |> put_if_present("return", Keyword.get(opts, :return))
  end

  def history_payload(id, opts) do
    %{"id" => id}
    |> put_if_present("partition_key", Keyword.get(opts, :partition_key))
    |> put_if_present("count", Keyword.get(opts, :count))
    |> put_if_present("from_event", Keyword.get(opts, :from_event))
    |> put_if_present("to_event", Keyword.get(opts, :to_event))
    |> put_if_present("from_ms", Keyword.get(opts, :from_ms))
    |> put_if_present("to_ms", Keyword.get(opts, :to_ms))
    |> put_if_present("from_version", Keyword.get(opts, :from_version))
    |> put_if_present("to_version", Keyword.get(opts, :to_version))
    |> put_if_present("rev", Keyword.get(opts, :rev))
    |> put_if_present("event", Keyword.get(opts, :event))
    |> put_if_present("worker", Keyword.get(opts, :worker))
    |> put_if_present("values", Keyword.get(opts, :values))
    |> put_if_present("payload_max_bytes", Keyword.get(opts, :payload_max_bytes))
    |> put_if_present("include_cold", Keyword.get(opts, :include_cold))
    |> put_if_present("consistent_projection", Keyword.get(opts, :consistent_projection))
  end

  def claim_due_payload(type, opts) do
    %{
      "type" => type,
      "worker" => Keyword.fetch!(opts, :worker),
      "lease_ms" => Keyword.get(opts, :lease_ms, 30_000),
      "limit" => Keyword.get(opts, :limit, 1),
      "return" => claim_payload_return_mode(opts)
    }
    |> put_if_present("now_ms", Keyword.get(opts, :now_ms))
    |> put_claim_state(opts)
    |> put_if_present("partition_key", Keyword.get(opts, :partition_key))
    |> put_if_present("partition_keys", Keyword.get(opts, :partition_keys))
    |> put_if_present("priority", Keyword.get(opts, :priority))
    |> put_if_present("block_ms", Keyword.get(opts, :block_ms))
    |> put_if_present("payload", Keyword.get(opts, :payload))
    |> put_if_present("payload_max_bytes", Keyword.get(opts, :payload_max_bytes))
    |> put_if_present("values", Keyword.get(opts, :values))
    |> put_if_present("value_max_bytes", Keyword.get(opts, :value_max_bytes))
    |> put_if_present("reclaim_expired", Keyword.get(opts, :reclaim_expired))
    |> put_if_present("reclaim_ratio", Keyword.get(opts, :reclaim_ratio))
  end

  def search_payload(opts) do
    %{"type" => Keyword.fetch!(opts, :type)}
    |> put_if_present("state", Keyword.get(opts, :state))
    |> put_if_present("partition_key", Keyword.get(opts, :partition_key))
    |> put_if_present("count", Keyword.get(opts, :count))
    |> put_if_present("from_ms", Keyword.get(opts, :from_ms))
    |> put_if_present("to_ms", Keyword.get(opts, :to_ms))
    |> put_if_present("rev", Keyword.get(opts, :rev))
    |> put_if_present("terminal_only", Keyword.get(opts, :terminal_only))
    |> put_if_present("consistent_projection", Keyword.get(opts, :consistent_projection))
    |> put_if_present("attributes", stringify_map(Keyword.get(opts, :attributes)))
    |> put_if_present("state_meta", Policy.normalize_search_state_meta(opts))
  end

  defp put_claim_state(map, opts) do
    case {Keyword.get(opts, :states), Keyword.get(opts, :state)} do
      {states, _state} when is_list(states) -> Map.put(map, "states", states)
      {nil, nil} -> map
      {nil, state} -> Map.put(map, "state", state)
      {states, _state} -> Map.put(map, "states", states)
    end
  end

  defp claim_return_mode(false, false), do: "JOBS_COMPACT"
  defp claim_return_mode(true, false), do: "JOBS_COMPACT_STATE"
  defp claim_return_mode(false, true), do: "JOBS_COMPACT_ATTRS"
  defp claim_return_mode(true, true), do: "JOBS_COMPACT_STATE_ATTRS"

  defp claim_payload_return_mode(opts) do
    if Keyword.get(opts, :include_record, false) do
      "RECORDS"
    else
      claim_return_mode(
        Keyword.get(opts, :include_state, false),
        Keyword.get(opts, :include_attributes, true)
      )
    end
  end
end
