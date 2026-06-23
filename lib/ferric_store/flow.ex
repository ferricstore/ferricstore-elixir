defmodule FerricStore.Flow do
  @moduledoc """
  High-level FerricFlow command helpers.

  Functions build the same native command arguments as the Python SDK while
  keeping defaults simple: create returns an ack, claim returns compact jobs with
  attributes, and terminal commands return an ack unless `return_record: true`.
  """

  alias FerricStore.Client
  alias FerricStore.Codec.Raw
  alias FerricStore.Protocol

  def create(client, id, opts),
    do: Client.native(client, Protocol.opcode(:flow_create), create_payload(id, opts))

  def enqueue(client, id, opts), do: create(client, id, Keyword.put_new(opts, :state, "queued"))

  def get(client, id, opts \\ []) do
    args = [id]
    args = append(args, "PARTITION", Keyword.get(opts, :partition_key))
    args = append_bool(args, "PAYLOAD", Keyword.get(opts, :payload))
    args = append(args, "PAYLOAD_MAX_BYTES", Keyword.get(opts, :payload_max_bytes))
    args = append_values(args, Keyword.get(opts, :values), Keyword.get(opts, :value_max_bytes))
    Client.command(client, "FLOW.GET", args)
  end

  def list(client, opts \\ []), do: Client.command(client, "FLOW.LIST", list_args(opts))

  def history(client, id, opts \\ []) do
    args = [id]
    args = append(args, "PARTITION", Keyword.get(opts, :partition_key))
    args = append_bool(args, "VALUES", Keyword.get(opts, :values))
    Client.command(client, "FLOW.HISTORY", args)
  end

  def claim_due(client, type, opts) do
    case Client.command(client, "FLOW.CLAIM_DUE", claim_due_args(type, opts)) do
      {:error, _error} = error -> error
      jobs when is_list(jobs) -> Enum.map(jobs, &normalize_claim_job/1)
      other -> other
    end
  end

  def transition(client, id, opts),
    do: Client.command(client, "FLOW.TRANSITION", transition_args(id, opts))

  def complete(client, id, opts),
    do: Client.native(client, Protocol.opcode(:flow_complete), complete_payload(id, opts))

  def retry(client, id, opts), do: Client.command(client, "FLOW.RETRY", retry_args(id, opts))
  def fail(client, id, opts), do: Client.command(client, "FLOW.FAIL", fail_args(id, opts))
  def cancel(client, id, opts), do: Client.command(client, "FLOW.CANCEL", cancel_args(id, opts))

  def value_put(client, value, opts \\ []) do
    codec = Keyword.get(opts, :codec, Raw)

    payload =
      %{"value" => codec.encode(value), "now_ms" => Keyword.get(opts, :now_ms, now_ms())}
      |> put_if_present("partition_key", Keyword.get(opts, :partition_key))
      |> put_if_present("owner_flow_id", Keyword.get(opts, :owner_flow_id))
      |> put_if_present("name", Keyword.get(opts, :name))
      |> put_if_present("override", Keyword.get(opts, :override))
      |> put_if_present("ttl_ms", Keyword.get(opts, :ttl_ms))
      |> put_if_present("local_cache", Keyword.get(opts, :local_cache))

    Client.native(client, Protocol.opcode(:flow_value_put), payload)
  end

  def value_mget(client, refs, opts \\ []) when is_list(refs) do
    args = append(refs, "MAX_BYTES", Keyword.get(opts, :max_bytes))
    Client.command(client, "FLOW.VALUE.MGET", args)
  end

  def signal(client, id, opts) do
    args = [id, "SIGNAL", Keyword.fetch!(opts, :signal)]
    args = append(args, "PARTITION", Keyword.get(opts, :partition_key))
    args = append(args, "IDEMPOTENCY", Keyword.get(opts, :idempotency_key))
    args = append_many(args, "IF_STATE", Keyword.get(opts, :if_state))
    args = append(args, "TRANSITION_TO", Keyword.get(opts, :transition_to))
    args = append(args, "RUN_AT", Keyword.get(opts, :run_at_ms))
    args = append(args, "NOW", Keyword.get(opts, :now_ms, now_ms()))
    args = append(args, "PRIORITY", Keyword.get(opts, :priority))
    args = append_named_values(args, Keyword.get(opts, :codec, Raw), opts)
    Client.command(client, "FLOW.SIGNAL", args)
  end

  def create_args(id, opts) do
    codec = Keyword.get(opts, :codec, Raw)
    now = Keyword.get(opts, :now_ms, now_ms())

    args = [
      id,
      "TYPE",
      Keyword.fetch!(opts, :type),
      "STATE",
      Keyword.get(opts, :state, "queued"),
      "NOW",
      now
    ]

    args = append(args, "PARTITION", Keyword.get(opts, :partition_key))
    args = append_encoded(args, "PAYLOAD", codec, Keyword.get(opts, :payload))
    args = append(args, "PARENT_FLOW_ID", Keyword.get(opts, :parent_flow_id))
    args = append(args, "ROOT_FLOW_ID", Keyword.get(opts, :root_flow_id))
    args = append(args, "CORRELATION_ID", Keyword.get(opts, :correlation_id))
    args = append(args, "RUN_AT", Keyword.get(opts, :run_at_ms, now))
    args = append(args, "PRIORITY", Keyword.get(opts, :priority))
    args = append_bool(args, "IDEMPOTENT", Keyword.get(opts, :idempotent))
    args = append(args, "RETENTION_TTL_MS", Keyword.get(opts, :retention_ttl_ms))
    args = append_attributes(args, opts)
    append_named_values(args, codec, opts)
  end

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
    |> put_if_present(
      "parent_id",
      Keyword.get(opts, :parent_flow_id) || Keyword.get(opts, :parent_id)
    )
    |> put_if_present("root_id", Keyword.get(opts, :root_flow_id) || Keyword.get(opts, :root_id))
    |> put_if_present("correlation_id", Keyword.get(opts, :correlation_id))
    |> put_if_present("priority", Keyword.get(opts, :priority))
    |> put_if_present("retention_ttl_ms", Keyword.get(opts, :retention_ttl_ms))
    |> put_if_present("attributes", stringify_map(Keyword.get(opts, :attributes)))
    |> put_if_present("values", encode_value_map(codec, Keyword.get(opts, :values)))
    |> put_if_present("value_refs", stringify_map(Keyword.get(opts, :value_refs)))
  end

  def claim_due_args(type, opts) do
    args = [type]
    args = append_many(args, "STATE", Keyword.get(opts, :states) || Keyword.get(opts, :state))

    args =
      args ++
        [
          "WORKER",
          Keyword.fetch!(opts, :worker),
          "LEASE_MS",
          Keyword.get(opts, :lease_ms, 30_000),
          "LIMIT",
          Keyword.get(opts, :limit, 1)
        ]

    args = append(args, "NOW", Keyword.get(opts, :now_ms))
    args = append(args, "PARTITION", Keyword.get(opts, :partition_key))

    args =
      case Keyword.get(opts, :partition_keys) do
        nil -> args
        keys -> args ++ ["PARTITIONS", length(keys)] ++ keys
      end

    args = append(args, "PRIORITY", Keyword.get(opts, :priority))

    args =
      if Keyword.get(opts, :include_record, false) do
        args
      else
        append(
          args,
          "RETURN",
          claim_return_mode(
            Keyword.get(opts, :include_state, false),
            Keyword.get(opts, :include_attributes, true)
          )
        )
      end

    args = append(args, "BLOCK", Keyword.get(opts, :block_ms))
    args = append_bool(args, "PAYLOAD", Keyword.get(opts, :payload))
    args = append(args, "PAYLOAD_MAX_BYTES", Keyword.get(opts, :payload_max_bytes))
    args = append_values(args, Keyword.get(opts, :values), Keyword.get(opts, :value_max_bytes))
    args = append_bool(args, "RECLAIM_EXPIRED", Keyword.get(opts, :reclaim_expired))
    append(args, "RECLAIM_RATIO", Keyword.get(opts, :reclaim_ratio))
  end

  def transition_args(id, opts) do
    codec = Keyword.get(opts, :codec, Raw)
    now = Keyword.get(opts, :now_ms, now_ms())

    args = [
      id,
      Keyword.fetch!(opts, :from_state),
      Keyword.fetch!(opts, :to_state),
      "LEASE_TOKEN",
      Keyword.fetch!(opts, :lease_token),
      "FENCING",
      Keyword.fetch!(opts, :fencing_token),
      "NOW",
      now
    ]

    args = append(args, "PARTITION", Keyword.get(opts, :partition_key))
    args = append_encoded(args, "PAYLOAD", codec, Keyword.get(opts, :payload))
    args = append(args, "RUN_AT", Keyword.get(opts, :run_at_ms, now))
    args = append(args, "PRIORITY", Keyword.get(opts, :priority))
    args = append_attributes(args, opts)
    append_named_values(args, codec, opts)
  end

  def complete_args(id, opts), do: terminal_args(id, opts, "RESULT")
  def complete_payload(id, opts), do: terminal_payload(id, opts, "result")

  def retry_args(id, opts) do
    id
    |> terminal_args(opts, "ERROR")
    |> append("RUN_AT", Keyword.get(opts, :run_at_ms))
  end

  def fail_args(id, opts), do: terminal_args(id, opts, "ERROR")
  def cancel_args(id, opts), do: terminal_args(id, opts, "ERROR")

  defp terminal_args(id, opts, value_name) do
    codec = Keyword.get(opts, :codec, Raw)

    args = [
      id,
      Keyword.fetch!(opts, :lease_token),
      "FENCING",
      Keyword.fetch!(opts, :fencing_token),
      "NOW",
      Keyword.get(opts, :now_ms, now_ms())
    ]

    args = append(args, "PARTITION", Keyword.get(opts, :partition_key))

    args =
      append_encoded(
        args,
        value_name,
        codec,
        Keyword.get(opts, :result) || Keyword.get(opts, :error)
      )

    args = append_encoded(args, "PAYLOAD", codec, Keyword.get(opts, :payload))
    args = append(args, "TTL", Keyword.get(opts, :ttl_ms))
    args = append_attributes(args, opts)
    append_named_values(args, codec, opts)
  end

  defp terminal_payload(id, opts, value_name) do
    codec = Keyword.get(opts, :codec, Raw)

    %{
      "id" => id,
      "lease_token" => Keyword.fetch!(opts, :lease_token),
      "now_ms" => Keyword.get(opts, :now_ms, now_ms())
    }
    |> put_if_present("fencing_token", Keyword.get(opts, :fencing_token))
    |> put_if_present("partition_key", Keyword.get(opts, :partition_key))
    |> put_if_present(
      value_name,
      encoded_or_nil(codec, Keyword.get(opts, :result) || Keyword.get(opts, :error))
    )
    |> put_if_present("payload", encoded_or_nil(codec, Keyword.get(opts, :payload)))
    |> put_if_present("ttl_ms", Keyword.get(opts, :ttl_ms))
    |> put_if_present("attributes", stringify_map(Keyword.get(opts, :attributes)))
    |> put_if_present("attributes_merge", stringify_map(Keyword.get(opts, :attributes_merge)))
    |> put_if_present("attributes_delete", Keyword.get(opts, :attributes_delete))
    |> put_if_present("values", encode_value_map(codec, Keyword.get(opts, :values)))
    |> put_if_present("value_refs", stringify_map(Keyword.get(opts, :value_refs)))
    |> put_if_present("drop_values", Keyword.get(opts, :drop_values))
    |> put_if_present("override_values", Keyword.get(opts, :override_values))
  end

  defp list_args(opts) do
    []
    |> append("TYPE", Keyword.get(opts, :type))
    |> append("STATE", Keyword.get(opts, :state))
    |> append("PARTITION", Keyword.get(opts, :partition_key))
    |> append("COUNT", Keyword.get(opts, :count))
    |> append("FROM", Keyword.get(opts, :from_ms))
    |> append("TO", Keyword.get(opts, :to_ms))
    |> append_bool("REV", Keyword.get(opts, :rev))
    |> append_attribute_filters(Keyword.get(opts, :attributes))
  end

  defp append(args, _name, nil), do: args
  defp append(args, name, value), do: args ++ [name, value]

  defp put_if_present(map, _key, nil), do: map
  defp put_if_present(map, key, value), do: Map.put(map, key, value)

  defp append_bool(args, _name, nil), do: args
  defp append_bool(args, name, true), do: args ++ [name, true]
  defp append_bool(args, name, false), do: args ++ [name, false]

  defp append_encoded(args, _name, _codec, nil), do: args
  defp append_encoded(args, name, codec, value), do: args ++ [name, codec.encode(value)]

  defp encoded_or_nil(_codec, nil), do: nil
  defp encoded_or_nil(codec, value), do: codec.encode(value)

  defp stringify_map(nil), do: nil

  defp stringify_map(map) when is_map(map),
    do: Map.new(map, fn {key, value} -> {to_string(key), value} end)

  defp encode_value_map(_codec, nil), do: nil

  defp encode_value_map(codec, map) when is_map(map) do
    Map.new(map, fn {key, value} -> {to_string(key), codec.encode(value)} end)
  end

  defp append_many(args, _name, nil), do: args

  defp append_many(args, name, values) when is_list(values),
    do: Enum.reduce(values, args, &append(&2, name, &1))

  defp append_many(args, name, value), do: append(args, name, value)

  defp append_values(args, nil, nil), do: args

  defp append_values(args, values, max_bytes) do
    args = append_many(args, "VALUE", values)
    append(args, "VALUE_MAX_BYTES", max_bytes)
  end

  defp append_attributes(args, opts) do
    args = append_attribute_filters(args, Keyword.get(opts, :attributes), "ATTRIBUTE")
    args = append_attribute_filters(args, Keyword.get(opts, :attributes_merge), "ATTRIBUTE_MERGE")
    append_many(args, "ATTRIBUTE_DELETE", Keyword.get(opts, :attributes_delete))
  end

  defp append_attribute_filters(args, attributes, prefix \\ "ATTRIBUTE")
  defp append_attribute_filters(args, nil, _prefix), do: args

  defp append_attribute_filters(args, attributes, prefix) when is_map(attributes) do
    Enum.reduce(attributes, args, fn {name, value}, acc ->
      acc ++ [prefix, to_string(name), value]
    end)
  end

  defp append_named_values(args, codec, opts) do
    args =
      Enum.reduce(Keyword.get(opts, :values, %{}) || %{}, args, fn {name, value}, acc ->
        acc ++ ["VALUE", to_string(name), codec.encode(value)]
      end)

    args =
      Enum.reduce(Keyword.get(opts, :value_refs, %{}) || %{}, args, fn {name, ref}, acc ->
        acc ++ ["VALUE_REF", to_string(name), ref]
      end)

    args = append_many(args, "DROP_VALUE", Keyword.get(opts, :drop_values))
    append_many(args, "OVERRIDE_VALUE", Keyword.get(opts, :override_values))
  end

  defp claim_return_mode(false, false), do: "JOBS_COMPACT"
  defp claim_return_mode(true, false), do: "JOBS_COMPACT_STATE"
  defp claim_return_mode(false, true), do: "JOBS_COMPACT_ATTRS"
  defp claim_return_mode(true, true), do: "JOBS_COMPACT_STATE_ATTRS"

  defp normalize_claim_job([id, partition_key, lease_token, fencing_token, attributes]) do
    %{
      "id" => id,
      "partition_key" => partition_key,
      "lease_token" => lease_token,
      "fencing_token" => fencing_token,
      "attributes" => attributes
    }
  end

  defp normalize_claim_job([id, partition_key, lease_token, fencing_token]) do
    %{
      "id" => id,
      "partition_key" => partition_key,
      "lease_token" => lease_token,
      "fencing_token" => fencing_token
    }
  end

  defp normalize_claim_job([id, lease_token, fencing_token]) do
    %{"id" => id, "lease_token" => lease_token, "fencing_token" => fencing_token}
  end

  defp normalize_claim_job(job), do: job

  defp now_ms, do: System.system_time(:millisecond)
end
