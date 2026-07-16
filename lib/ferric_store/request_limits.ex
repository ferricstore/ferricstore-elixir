defmodule FerricStore.RequestLimits do
  @moduledoc false

  alias FerricStore.{BoundedList, DeadlineBudget}
  alias FerricStore.Protocol.CommandSpec

  @max_batch_items 100_000
  @max_command_items 100_000
  @max_group_concurrency 256
  @internal_options [:__batch_item_count__, :__client_deadline__]

  @spec max_batch_items() :: pos_integer()
  def max_batch_items, do: @max_batch_items

  @spec max_command_items() :: pos_integer()
  def max_command_items, do: @max_command_items

  @spec max_group_concurrency() :: pos_integer()
  def max_group_concurrency, do: @max_group_concurrency

  @spec valid_configured_batch_limit?(term()) :: boolean()
  def valid_configured_batch_limit?(nil), do: true

  def valid_configured_batch_limit?(value),
    do: is_integer(value) and value > 0 and value <= @max_batch_items

  @doc false
  @spec prepare(non_neg_integer(), term(), keyword()) ::
          {:ok, keyword(), non_neg_integer() | nil}
          | {:error, {:batch_too_large, map()} | {:invalid_request_payload, map()}}
  def prepare(opcode, payload, opts) when is_list(opts) do
    opts = Keyword.drop(opts, @internal_options)

    case payload_item_count(opcode, payload, @max_batch_items) do
      {:ok, count} ->
        {:ok, opts, count}

      {:error, {:limit_exceeded, observed}} ->
        batch_error(observed, @max_batch_items)

      {:error, :improper_list} ->
        invalid_payload_error(:improper_list)

      {:error, {:duplicate_normalized_batch_field, field}} ->
        invalid_payload_error(:duplicate_normalized_batch_field, %{field: field})
    end
  end

  @doc false
  @spec prepare(non_neg_integer(), term(), keyword(), DeadlineBudget.t()) ::
          {:ok, keyword(), non_neg_integer() | nil}
          | {:error, :timeout | {:batch_too_large, map()} | {:invalid_request_payload, map()}}
  def prepare(opcode, payload, opts, %DeadlineBudget{} = budget) when is_list(opts) do
    opts = Keyword.drop(opts, @internal_options)

    case payload_item_count(opcode, payload, @max_batch_items, budget) do
      {:ok, count} ->
        {:ok, opts, count}

      {:error, {:limit_exceeded, observed}} ->
        batch_error(observed, @max_batch_items)

      {:error, :improper_list} ->
        invalid_payload_error(:improper_list)

      {:error, {:duplicate_normalized_batch_field, field}} ->
        invalid_payload_error(:duplicate_normalized_batch_field, %{field: field})

      {:error, :timeout} = error ->
        error
    end
  end

  @doc false
  @spec admit(non_neg_integer() | nil, pos_integer()) ::
          :ok | {:error, {:batch_too_large, map()}}
  def admit(nil, limit) when is_integer(limit) and limit > 0 and limit <= @max_batch_items,
    do: :ok

  def admit(count, limit)
      when is_integer(count) and count >= 0 and is_integer(limit) and limit > 0 and
             limit <= @max_batch_items do
    case configured_bounded_count(count, limit) do
      {:ok, _count} -> :ok
      {:error, observed} -> batch_error(observed, limit)
    end
  end

  defp payload_item_count(opcode, payload, limit) when is_map(payload) do
    case CommandSpec.batch(opcode) do
      %{field: string_field, atom_field: atom_field, type: collection_type} ->
        case batch_collection(payload, string_field, atom_field) do
          {:ok, collection} -> collection_count(collection, collection_type, limit)
          :missing -> {:ok, nil}
          {:error, _reason} = error -> error
        end

      _not_a_batch_command ->
        {:ok, nil}
    end
  end

  defp payload_item_count(_opcode, _payload, _limit), do: {:ok, nil}

  defp payload_item_count(opcode, payload, limit, budget) when is_map(payload) do
    with :ok <- DeadlineBudget.ensure_active(budget) do
      budgeted_payload_item_count(CommandSpec.batch(opcode), payload, limit, budget)
    end
  end

  defp payload_item_count(_opcode, _payload, _limit, budget),
    do: deadline_result(budget, nil)

  defp budgeted_payload_item_count(
         %{field: string_field, atom_field: atom_field, type: collection_type},
         payload,
         limit,
         budget
       ) do
    case batch_collection(payload, string_field, atom_field) do
      {:ok, collection} -> collection_count(collection, collection_type, limit, budget)
      :missing -> deadline_result(budget, nil)
      {:error, _reason} = error -> error
    end
  end

  defp budgeted_payload_item_count(_not_a_batch_command, _payload, _limit, budget),
    do: deadline_result(budget, nil)

  defp batch_collection(payload, string_field, atom_field) do
    case {Map.fetch(payload, string_field), Map.fetch(payload, atom_field)} do
      {{:ok, _string_value}, {:ok, _atom_value}} ->
        {:error, {:duplicate_normalized_batch_field, string_field}}

      {{:ok, nil}, :error} ->
        :missing

      {{:ok, collection}, :error} ->
        {:ok, collection}

      {:error, {:ok, nil}} ->
        :missing

      {:error, {:ok, collection}} ->
        {:ok, collection}

      {:error, :error} ->
        :missing
    end
  end

  defp bounded_count(count, limit) when count <= limit, do: {:ok, count}
  defp bounded_count(count, _limit), do: {:error, count}

  defp configured_bounded_count(count, limit) when count <= limit, do: {:ok, count}
  defp configured_bounded_count(_count, limit), do: {:error, limit + 1}

  defp collection_count(items, :list, limit) when is_list(items),
    do: BoundedList.count(items, limit)

  defp collection_count(fields, :map, limit) when is_map(fields),
    do: fields |> map_size() |> bounded_count(limit)

  defp collection_count(_collection, _collection_type, _limit), do: {:ok, nil}

  defp collection_count(items, :list, limit, budget) when is_list(items),
    do: BoundedList.count(items, limit, budget)

  defp collection_count(fields, :map, limit, budget) when is_map(fields) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         result <- fields |> map_size() |> bounded_count(limit),
         :ok <- DeadlineBudget.ensure_active(budget) do
      result
    end
  end

  defp collection_count(_collection, _collection_type, _limit, budget),
    do: deadline_result(budget, nil)

  defp deadline_result(budget, result) do
    with :ok <- DeadlineBudget.ensure_active(budget), do: {:ok, result}
  end

  defp batch_error(observed, limit),
    do: {:error, {:batch_too_large, %{items: observed, limit: limit}}}

  defp invalid_payload_error(reason, details \\ %{}),
    do: {:error, {:invalid_request_payload, Map.put(details, :reason, reason)}}
end
