defmodule FerricStore.Protocol.FlowCreateBatchCodec do
  @moduledoc false

  alias FerricStore.Protocol.{FlowBatchFields, FlowCreateBatchItems}

  @create_many_keys MapSet.new([
                      "items",
                      "type",
                      "state",
                      "now_ms",
                      "run_at_ms",
                      "partition_key",
                      "independent",
                      "return"
                    ])

  def create_many_payload(payload),
    do: payload |> create_many_iodata_payload() |> FlowBatchFields.flatten()

  def create_many_iodata_payload(payload), do: create_many_iodata_payload(payload, :count_items)

  def create_many_iodata_payload(
        %{
          "type" => type,
          "state" => state,
          "now_ms" => now_ms,
          "run_at_ms" => run_at_ms,
          "items" => items
        } = payload,
        item_count
      )
      when is_binary(type) and is_binary(state) and is_integer(now_ms) and is_integer(run_at_ms) and
             is_list(items) do
    with :ok <- FlowBatchFields.payload_keys(payload, @create_many_keys),
         :ok <- FlowBatchFields.signed_64(now_ms),
         :ok <- FlowBatchFields.signed_64(run_at_ms),
         {:ok, return_mode} <- FlowBatchFields.return_mode(Map.get(payload, "return")),
         {:ok, independent_marker} <-
           FlowBatchFields.optional_boolean_marker(Map.get(payload, "independent")),
         {:ok, partition_key} <-
           FlowBatchFields.optional_binary_value(Map.get(payload, "partition_key")),
         {:ok, item_count} <- FlowBatchFields.collection_length(items, item_count),
         {:ok, item_bytes, tag, encoded_count} <-
           FlowCreateBatchItems.create(items, partition_key, item_count),
         true <- item_count == encoded_count do
      prefix = [<<tag>>, FlowBatchFields.binary(type), FlowBatchFields.binary(state)]
      partition = if partition_key, do: FlowBatchFields.optional_binary(partition_key), else: []

      {:ok,
       [
         prefix,
         partition,
         <<
           now_ms::signed-64,
           run_at_ms::signed-64,
           independent_marker::8,
           return_mode::8,
           item_count::32
         >>,
         item_bytes
       ]}
    else
      _invalid -> :error
    end
  end

  def create_many_iodata_payload(_payload, _item_count), do: :error

  def create_many_ids_payload(type, state, partition_key, ids, opts \\ []) do
    type
    |> create_many_ids_iodata_payload(state, partition_key, ids, opts)
    |> FlowBatchFields.flatten()
  end

  def create_many_ids_iodata_payload(type, state, partition_key, ids, opts \\ [])

  def create_many_ids_iodata_payload(type, state, partition_key, ids, opts)
      when is_binary(type) and is_binary(state) and is_list(ids) do
    now_ms = Keyword.get(opts, :now_ms, System.system_time(:millisecond))
    run_at_ms = Keyword.get(opts, :run_at_ms, now_ms)

    with :ok <- FlowBatchFields.signed_64(now_ms),
         :ok <- FlowBatchFields.signed_64(run_at_ms),
         {:ok, return_mode} <-
           FlowBatchFields.return_mode(
             if(Keyword.get(opts, :return_ok_on_success), do: "OK_ON_SUCCESS")
           ),
         {:ok, independent_marker} <-
           FlowBatchFields.optional_boolean_marker(Keyword.get(opts, :independent)),
         {:ok, partition_key} <- FlowBatchFields.optional_binary_value(partition_key),
         {:ok, item_count} <- FlowBatchFields.bounded_collection_length(ids),
         {:ok, item_bytes, tag} <- FlowCreateBatchItems.ids(ids, partition_key) do
      partition = if partition_key, do: FlowBatchFields.optional_binary(partition_key), else: []

      {:ok,
       [
         <<tag>>,
         FlowBatchFields.binary(type),
         FlowBatchFields.binary(state),
         partition,
         <<
           now_ms::signed-64,
           run_at_ms::signed-64,
           independent_marker::8,
           return_mode::8,
           item_count::32
         >>,
         item_bytes
       ]}
    end
  end

  def create_many_ids_iodata_payload(_type, _state, _partition_key, _ids, _opts), do: :error
end
