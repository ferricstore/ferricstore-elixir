defmodule FerricStore.Flow.Payload.Mutation do
  @moduledoc false

  import FerricStore.Flow.Payload.Normalize

  alias FerricStore.Codec.Raw
  alias FerricStore.Flow.Payload.Policy

  def create_payload(id, opts) do
    codec = Keyword.get(opts, :codec, Raw)
    now = Keyword.get(opts, :now_ms, now_ms())

    %{
      "id" => id,
      "type" => Keyword.fetch!(opts, :type),
      "state" => Keyword.get(opts, :state, "queued"),
      "now_ms" => now,
      "run_at_ms" => Keyword.get(opts, :run_at_ms, now)
    }
    |> put_if_present("partition_key", Keyword.get(opts, :partition_key))
    |> put_if_present("payload", encoded_or_nil(codec, Keyword.get(opts, :payload)))
    |> put_if_present("payload_ref", Keyword.get(opts, :payload_ref))
    |> put_if_present("parent_flow_id", Keyword.get(opts, :parent_flow_id))
    |> put_if_present("root_flow_id", Keyword.get(opts, :root_flow_id))
    |> put_if_present("correlation_id", Keyword.get(opts, :correlation_id))
    |> put_if_present("priority", Keyword.get(opts, :priority))
    |> put_if_present("idempotent", Keyword.get(opts, :idempotent))
    |> put_if_present("retention_ttl_ms", Keyword.get(opts, :retention_ttl_ms))
    |> put_if_present("max_active_ms", Keyword.get(opts, :max_active_ms))
    |> put_if_present("history_hot_max_events", Keyword.get(opts, :history_hot_max_events))
    |> put_if_present("history_max_events", Keyword.get(opts, :history_max_events))
    |> put_if_present("attributes", stringify_map(Keyword.get(opts, :attributes)))
    |> put_if_present("state_meta", stringify_nested_map(Keyword.get(opts, :state_meta)))
    |> put_if_present("values", encode_value_map(codec, Keyword.get(opts, :values)))
    |> put_if_present("value_refs", stringify_map(Keyword.get(opts, :value_refs)))
  end

  def transition_payload(id, opts) do
    codec = Keyword.get(opts, :codec, Raw)
    now = Keyword.get(opts, :now_ms, now_ms())

    %{
      "id" => id,
      "from_state" => Keyword.fetch!(opts, :from_state),
      "to_state" => Keyword.fetch!(opts, :to_state),
      "lease_token" => Keyword.fetch!(opts, :lease_token),
      "fencing_token" => Keyword.fetch!(opts, :fencing_token),
      "now_ms" => now
    }
    |> put_if_present("partition_key", Keyword.get(opts, :partition_key))
    |> put_if_present("payload", encoded_or_nil(codec, Keyword.get(opts, :payload)))
    |> put_if_present("run_at_ms", Keyword.get(opts, :run_at_ms, now))
    |> put_if_present("priority", Keyword.get(opts, :priority))
    |> put_if_present("attributes", stringify_map(Keyword.get(opts, :attributes)))
    |> put_if_present("attributes_merge", stringify_map(Keyword.get(opts, :attributes_merge)))
    |> put_if_present("attributes_delete", Keyword.get(opts, :attributes_delete))
    |> put_if_present("state_meta", stringify_nested_map(Keyword.get(opts, :state_meta)))
    |> put_if_present("values", encode_value_map(codec, Keyword.get(opts, :values)))
    |> put_if_present("value_refs", stringify_map(Keyword.get(opts, :value_refs)))
    |> put_if_present("drop_values", Keyword.get(opts, :drop_values))
    |> put_if_present("override_values", Keyword.get(opts, :override_values))
  end

  def complete_payload(id, opts), do: terminal_payload(id, opts, "result", :result)

  def retry_payload(id, opts) do
    codec = Keyword.get(opts, :codec, Raw)

    %{
      "id" => id,
      "lease_token" => Keyword.fetch!(opts, :lease_token),
      "fencing_token" => Keyword.fetch!(opts, :fencing_token),
      "now_ms" => Keyword.get(opts, :now_ms, now_ms())
    }
    |> put_if_present("partition_key", Keyword.get(opts, :partition_key))
    |> put_if_present("error", encoded_or_nil(codec, Keyword.get(opts, :error)))
    |> put_if_present("payload", encoded_or_nil(codec, Keyword.get(opts, :payload)))
    |> put_if_present("run_at_ms", Keyword.get(opts, :run_at_ms))
    |> put_if_present("retry", Policy.normalize_policy_value(Keyword.get(opts, :retry)))
    |> put_if_present("attributes", stringify_map(Keyword.get(opts, :attributes)))
    |> put_if_present("attributes_merge", stringify_map(Keyword.get(opts, :attributes_merge)))
    |> put_if_present("attributes_delete", Keyword.get(opts, :attributes_delete))
    |> put_if_present("state_meta", stringify_nested_map(Keyword.get(opts, :state_meta)))
  end

  def fail_payload(id, opts), do: terminal_payload(id, opts, "error", :error)

  def cancel_payload(id, opts) do
    codec = Keyword.get(opts, :codec, Raw)

    %{
      "id" => id,
      "fencing_token" => Keyword.fetch!(opts, :fencing_token),
      "now_ms" => Keyword.get(opts, :now_ms, now_ms())
    }
    |> put_if_present("lease_token", Keyword.get(opts, :lease_token))
    |> put_if_present("partition_key", Keyword.get(opts, :partition_key))
    |> put_if_present("reason", encoded_or_nil(codec, Keyword.get(opts, :reason)))
    |> put_if_present("ttl_ms", Keyword.get(opts, :ttl_ms))
    |> put_if_present("attributes", stringify_map(Keyword.get(opts, :attributes)))
    |> put_if_present("attributes_merge", stringify_map(Keyword.get(opts, :attributes_merge)))
    |> put_if_present("attributes_delete", Keyword.get(opts, :attributes_delete))
    |> put_if_present("state_meta", stringify_nested_map(Keyword.get(opts, :state_meta)))
    |> put_if_present("values", encode_value_map(codec, Keyword.get(opts, :values)))
    |> put_if_present("value_refs", stringify_map(Keyword.get(opts, :value_refs)))
    |> put_if_present("drop_values", Keyword.get(opts, :drop_values))
    |> put_if_present("override_values", Keyword.get(opts, :override_values))
  end

  def signal_payload(id, opts) do
    codec = Keyword.get(opts, :codec, Raw)

    %{
      "id" => id,
      "signal" => Keyword.fetch!(opts, :signal),
      "now_ms" => Keyword.get(opts, :now_ms, now_ms())
    }
    |> put_if_present("partition_key", Keyword.get(opts, :partition_key))
    |> put_if_present("idempotency_key", Keyword.get(opts, :idempotency_key))
    |> put_if_present("if_state", Keyword.get(opts, :if_state))
    |> put_if_present("transition_to", Keyword.get(opts, :transition_to))
    |> put_if_present("run_at_ms", Keyword.get(opts, :run_at_ms))
    |> put_if_present("values", encode_value_map(codec, Keyword.get(opts, :values)))
    |> put_if_present("value_refs", stringify_map(Keyword.get(opts, :value_refs)))
    |> put_if_present("drop_values", Keyword.get(opts, :drop_values))
    |> put_if_present("override_values", Keyword.get(opts, :override_values))
  end

  defp terminal_payload(id, opts, value_name, value_option) do
    codec = Keyword.get(opts, :codec, Raw)

    %{
      "id" => id,
      "lease_token" => Keyword.fetch!(opts, :lease_token),
      "fencing_token" => Keyword.fetch!(opts, :fencing_token),
      "now_ms" => Keyword.get(opts, :now_ms, now_ms())
    }
    |> put_if_present("partition_key", Keyword.get(opts, :partition_key))
    |> put_if_present(value_name, encoded_or_nil(codec, Keyword.get(opts, value_option)))
    |> put_if_present("payload", encoded_or_nil(codec, Keyword.get(opts, :payload)))
    |> put_if_present("ttl_ms", Keyword.get(opts, :ttl_ms))
    |> put_if_present("attributes", stringify_map(Keyword.get(opts, :attributes)))
    |> put_if_present("attributes_merge", stringify_map(Keyword.get(opts, :attributes_merge)))
    |> put_if_present("attributes_delete", Keyword.get(opts, :attributes_delete))
    |> put_if_present("state_meta", stringify_nested_map(Keyword.get(opts, :state_meta)))
    |> put_if_present("values", encode_value_map(codec, Keyword.get(opts, :values)))
    |> put_if_present("value_refs", stringify_map(Keyword.get(opts, :value_refs)))
    |> put_if_present("drop_values", Keyword.get(opts, :drop_values))
    |> put_if_present("override_values", Keyword.get(opts, :override_values))
  end
end
