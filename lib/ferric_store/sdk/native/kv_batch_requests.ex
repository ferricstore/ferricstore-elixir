defmodule FerricStore.SDK.Native.KVBatchRequests do
  @moduledoc false

  alias FerricStore.RequestContext

  alias FerricStore.SDK.Native.{
    ClientSupervisor,
    CoordinatorCall,
    KVBatchPreparer,
    KVPreparedRequest
  }

  @default_timeout 5_000
  @preparation_admission_threshold 256

  @spec dispatch(
          pid(),
          KVBatchPreparer.operation(),
          non_neg_integer(),
          list() | map(),
          non_neg_integer(),
          RequestContext.t()
        ) :: {:ok, [map()], non_neg_integer()} | {:error, term()}
  def dispatch(client, operation, opcode, items, item_count, %RequestContext{} = context) do
    case ClientSupervisor.topology_snapshot(client) do
      {:ok, version, topology} ->
        prepare_and_dispatch(
          client,
          operation,
          opcode,
          items,
          item_count,
          version,
          topology,
          context
        )

      {:error, _reason} ->
        dispatch_unprepared(client, operation, opcode, items, item_count, context)
    end
  end

  defp prepare_and_dispatch(
         client,
         operation,
         opcode,
         items,
         item_count,
         version,
         topology,
         context
       ) do
    with :ok <- RequestContext.ensure_active(context),
         {:ok, reservation} <- admit_preparation(client, item_count, context) do
      case KVBatchPreparer.prepare(
             topology,
             operation,
             items,
             context
           ) do
        {:ok, groups} ->
          submit_prepared(
            client,
            reservation,
            opcode,
            operation,
            item_count,
            version,
            groups,
            context
          )

        {:error, _reason} = error ->
          release_preparation(client, reservation)
          error
      end
    end
  end

  defp submit_prepared(
         client,
         reservation,
         opcode,
         operation,
         item_count,
         version,
         groups,
         context
       ) do
    prepared =
      KVPreparedRequest.new(
        reservation,
        opcode,
        operation,
        item_count,
        version,
        groups,
        context
      )

    result =
      with :ok <- RequestContext.ensure_active(context) do
        CoordinatorCall.submit(
          client,
          {:prepared_command_items, prepared},
          call_timeout(context)
        )
      end

    maybe_release_failed_preparation(client, reservation, result)
    normalize_group_result(result, item_count)
  end

  defp admit_preparation(client, item_count, context)
       when item_count >= @preparation_admission_threshold do
    CoordinatorCall.submit(
      client,
      {:kv_preparation_admission, item_count, context},
      call_timeout(context)
    )
  end

  defp admit_preparation(_client, _item_count, _context), do: {:ok, nil}

  defp maybe_release_failed_preparation(_client, _reservation, {:ok, _groups}), do: :ok

  defp maybe_release_failed_preparation(client, reservation, {:error, _reason}),
    do: release_preparation(client, reservation)

  defp release_preparation(_client, nil), do: :ok

  defp release_preparation(client, reservation) when is_reference(reservation) do
    _result = CoordinatorCall.cast(client, {:release_kv_preparation, reservation, self()})
    :ok
  end

  defp dispatch_unprepared(client, operation, opcode, items, item_count, context) do
    {key_fun, payload_builder} = KVBatchPreparer.callbacks(operation)

    result =
      with :ok <- RequestContext.ensure_active(context) do
        CoordinatorCall.submit(
          client,
          {:command_items, opcode, items, item_count, key_fun, payload_builder, context},
          call_timeout(context)
        )
      end

    normalize_group_result(result, item_count)
  end

  defp normalize_group_result({:ok, groups}, item_count), do: {:ok, groups, item_count}
  defp normalize_group_result({:error, _reason} = error, _item_count), do: error

  defp call_timeout(context), do: RequestContext.call_timeout(context, @default_timeout)
end
