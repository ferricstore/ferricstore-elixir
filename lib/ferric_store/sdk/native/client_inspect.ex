defimpl Inspect, for: FerricStore.SDK.Native.Coordinator.State do
  import Inspect.Algebra

  @redacted "[REDACTED]"

  def inspect(client, opts) do
    fields =
      client
      |> Map.from_struct()
      |> Map.put(:password, redacted(client.password))
      |> Map.update!(:topology_manager, &summarize_topology_manager/1)
      |> Map.update!(:request_registry, &redact_request_registry/1)
      |> Map.update!(:batch_scheduler, &summarize_batch_scheduler/1)

    concat(["#FerricStore.SDK.Native.Client<", to_doc(fields, opts), ">"])
  end

  defp redacted(nil), do: nil
  defp redacted(_value), do: @redacted

  defp topology_summary(nil), do: nil

  defp topology_summary(topology) do
    %{
      route_epoch: topology.route_epoch,
      shard_count: topology.shard_count,
      endpoint_count: map_size(topology.endpoints)
    }
  end

  defp summarize_topology_manager(%FerricStore.SDK.Native.TopologyManager{} = manager) do
    %{manager | topology: topology_summary(manager.topology)}
  end

  defp redact_pending_requests(requests) do
    Map.new(requests, fn {tag, request} ->
      request =
        request
        |> Map.put(:payload, @redacted)
        |> redact_nested_event_payload()
        |> redact_nested_batch_group()

      {tag, request}
    end)
  end

  defp redact_request_registry(%FerricStore.SDK.Native.RequestRegistry{} = registry) do
    %{registry | requests: redact_pending_requests(registry.requests)}
  end

  defp redact_nested_event_payload(%{event_call: event_call} = request)
       when is_map(event_call) do
    Map.put(request, :event_call, Map.delete(event_call, :events))
  end

  defp redact_nested_event_payload(request), do: request

  defp redact_nested_batch_group(%{group: group} = request) when is_map(group),
    do: Map.put(request, :group, @redacted)

  defp redact_nested_batch_group(request), do: request

  defp summarize_pending_batches(batches) do
    Map.new(batches, fn {id, batch} ->
      summary = %{
        opcode: batch.opcode,
        phase: batch.phase,
        attempt: batch.attempt,
        item_count: batch.item_count,
        connecting_group_count: map_size(batch.connecting_groups),
        queued_group_count: length(batch.queued),
        inflight: batch.inflight,
        success_count: length(batch.successes),
        failure_count: length(batch.failures)
      }

      {id, summary}
    end)
  end

  defp summarize_batch_scheduler(%FerricStore.SDK.Native.BatchScheduler{} = scheduler) do
    %{scheduler | batches: summarize_pending_batches(scheduler.batches)}
  end
end
