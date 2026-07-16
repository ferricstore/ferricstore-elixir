defmodule FerricStore.Flow.Payload.Batch do
  @moduledoc false

  import FerricStore.Flow.Payload.Normalize

  alias FerricStore.BoundedList
  alias FerricStore.Codec.Raw
  alias FerricStore.DeadlineBudget
  alias FerricStore.Flow.Payload.CreateManyItems
  alias FerricStore.RequestLimits
  alias FerricStore.Types

  import FerricStore.Protocol.ValueDomain, only: [is_signed_64_integer: 1]

  @max_many_items RequestLimits.max_batch_items()

  def create_many_payload(items, opts) when is_list(items) do
    case create_many_with_count(items, opts) do
      {:ok, payload, _item_count} -> payload
      {:error, _reason} = error -> error
    end
  end

  def create_many_payload(_items, _opts), do: {:error, {:invalid_batch_items, :expected_list}}

  def create_many_with_count(items, opts) when is_list(items) do
    create_many(items, opts, nil)
  end

  def create_many_with_count(_items, _opts),
    do: {:error, {:invalid_batch_items, :expected_list}}

  def create_many_with_count(items, opts, %DeadlineBudget{} = budget) when is_list(items) do
    create_many(items, opts, budget)
  end

  def create_many_with_count(_items, _opts, %DeadlineBudget{}),
    do: {:error, {:invalid_batch_items, :expected_list}}

  defp create_many(items, opts, budget) do
    codec = Keyword.get(opts, :codec, Raw)
    now = Keyword.get(opts, :now_ms, now_ms())

    case CreateManyItems.map(items, codec, @max_many_items, budget) do
      {:ok, item_count, mapped_items} ->
        payload =
          %{
            "items" => mapped_items,
            "type" => Keyword.fetch!(opts, :type),
            "state" => Keyword.get(opts, :state, "queued"),
            "now_ms" => now,
            "run_at_ms" => Keyword.get(opts, :run_at_ms, now)
          }
          |> put_if_present("partition_key", Keyword.get(opts, :partition_key))
          |> put_if_present("independent", Keyword.get(opts, :independent))
          |> put_if_present("return", many_return_mode(opts))
          |> put_if_present("idempotent", Keyword.get(opts, :idempotent))
          |> put_if_present("priority", Keyword.get(opts, :priority))
          |> put_if_present("retention_ttl_ms", Keyword.get(opts, :retention_ttl_ms))
          |> put_if_present("attributes", stringify_map(Keyword.get(opts, :attributes)))
          |> put_if_present("state_meta", stringify_nested_map(Keyword.get(opts, :state_meta)))
          |> put_if_present("values", encode_value_map(codec, Keyword.get(opts, :values)))
          |> put_if_present("value_refs", stringify_map(Keyword.get(opts, :value_refs)))

        {:ok, payload, item_count}

      {:error, {:limit_exceeded, observed}} ->
        batch_too_large(observed)

      {:error, :improper_list} ->
        {:error, {:invalid_batch_items, :improper_list}}

      {:error, _reason} = error ->
        error
    end
  end

  def complete_many_payload(jobs, opts) when is_list(jobs) do
    case complete_many_with_count(jobs, opts) do
      {:ok, payload, _item_count} -> payload
      {:error, _reason} = error -> error
    end
  end

  def complete_many_payload(_jobs, _opts), do: {:error, {:invalid_batch_items, :expected_list}}

  def complete_many_with_count(jobs, opts) when is_list(jobs) do
    complete_many(jobs, opts, nil)
  end

  def complete_many_with_count(_jobs, _opts),
    do: {:error, {:invalid_batch_items, :expected_list}}

  def complete_many_with_count(jobs, opts, %DeadlineBudget{} = budget) when is_list(jobs) do
    complete_many(jobs, opts, budget)
  end

  def complete_many_with_count(_jobs, _opts, %DeadlineBudget{}),
    do: {:error, {:invalid_batch_items, :expected_list}}

  defp complete_many(jobs, opts, budget) do
    codec = Keyword.get(opts, :codec, Raw)

    case map_results(jobs, &complete_many_item/1, budget) do
      {:ok, item_count, mapped_items} ->
        payload =
          %{
            "items" => mapped_items,
            "now_ms" => Keyword.get(opts, :now_ms, now_ms())
          }
          |> put_if_present("partition_key", Keyword.get(opts, :partition_key))
          |> put_if_present("independent", Keyword.get(opts, :independent))
          |> put_if_present("return", many_return_mode(opts))
          |> put_if_present("result", encoded_or_nil(codec, Keyword.get(opts, :result)))
          |> put_if_present("payload", encoded_or_nil(codec, Keyword.get(opts, :payload)))
          |> put_if_present("ttl_ms", Keyword.get(opts, :ttl_ms))
          |> put_if_present(
            "attributes_merge",
            stringify_map(Keyword.get(opts, :attributes_merge))
          )
          |> put_if_present("attributes_delete", Keyword.get(opts, :attributes_delete))
          |> put_if_present("state_meta", stringify_nested_map(Keyword.get(opts, :state_meta)))
          |> put_if_present("values", encode_value_map(codec, Keyword.get(opts, :values)))
          |> put_if_present("value_refs", stringify_map(Keyword.get(opts, :value_refs)))
          |> put_if_present("drop_values", Keyword.get(opts, :drop_values))
          |> put_if_present("override_values", Keyword.get(opts, :override_values))

        {:ok, payload, item_count}

      {:error, {:limit_exceeded, observed}} ->
        batch_too_large(observed)

      {:error, :improper_list} ->
        {:error, {:invalid_batch_items, :improper_list}}

      {:error, _reason} = error ->
        error
    end
  end

  defp map_results(items, mapper, nil),
    do: BoundedList.map_result_with_count(items, @max_many_items, mapper)

  defp map_results(items, mapper, %DeadlineBudget{} = budget),
    do: BoundedList.map_result_with_count(items, @max_many_items, mapper, budget)

  defp batch_too_large(observed),
    do: {:error, {:batch_too_large, %{items: observed, limit: @max_many_items}}}

  defp many_return_mode(opts) do
    if Keyword.get(opts, :return_ok_on_success, false), do: "OK_ON_SUCCESS"
  end

  defp complete_many_item(%{} = job) when map_size(job) <= 4 do
    with {:ok, normalized} <- Types.normalize_map_keys_result(job),
         true <-
           only_keys?(normalized, ["id", "partition_key", "lease_token", "fencing_token"]),
         id when is_binary(id) and id != "" <- Map.get(normalized, "id"),
         lease_token when is_binary(lease_token) and lease_token != "" <-
           Map.get(normalized, "lease_token"),
         fencing_token when is_signed_64_integer(fencing_token) and fencing_token >= 0 <-
           Map.get(normalized, "fencing_token"),
         partition_key <- Map.get(normalized, "partition_key"),
         true <- is_nil(partition_key) or (is_binary(partition_key) and partition_key != "") do
      if partition_key,
        do: {:ok, [id, partition_key, lease_token, fencing_token]},
        else: {:ok, [id, lease_token, fencing_token]}
    else
      _invalid -> invalid_complete_many_item(job)
    end
  end

  defp complete_many_item(%{} = job), do: invalid_complete_many_item(job)

  defp complete_many_item({id, lease_token, fencing_token})
       when is_binary(id) and id != "" and is_binary(lease_token) and lease_token != "" and
              is_signed_64_integer(fencing_token) and fencing_token >= 0,
       do: {:ok, [id, lease_token, fencing_token]}

  defp complete_many_item({id, partition_key, lease_token, fencing_token})
       when is_binary(id) and id != "" and is_binary(partition_key) and partition_key != "" and
              is_binary(lease_token) and lease_token != "" and
              is_signed_64_integer(fencing_token) and fencing_token >= 0,
       do: {:ok, [id, partition_key, lease_token, fencing_token]}

  defp complete_many_item(job), do: invalid_complete_many_item(job)

  defp only_keys?(map, allowed), do: Enum.all?(Map.keys(map), &(&1 in allowed))

  defp invalid_complete_many_item(item),
    do: {:error, {:invalid_flow_complete_many_item, item}}
end
