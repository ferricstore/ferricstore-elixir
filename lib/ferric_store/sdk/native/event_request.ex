defmodule FerricStore.SDK.Native.EventRequest do
  @moduledoc false

  alias FerricStore.Protocol.CommandSpec
  alias FerricStore.RequestContext
  alias FerricStore.SDK.Native.EventSubscriptions

  @op_subscribe_events CommandSpec.fetch!(:subscribe_events).opcode
  @op_unsubscribe_events CommandSpec.fetch!(:unsubscribe_events).opcode
  @restore_initial_backoff 50
  @restore_max_backoff 1_000

  @spec restore(EventSubscriptions.t(), RequestContext.t(), reference()) :: map()
  def restore(subscriptions, %RequestContext{} = opts, token) do
    events = subscriptions |> EventSubscriptions.desired_events() |> wire_payload()

    %{
      kind: :event_restore,
      from: nil,
      opcode: @op_subscribe_events,
      key: nil,
      payload: %{"events" => events},
      opts: opts,
      attempt: 0,
      original_reason: nil,
      restore_token: token
    }
  end

  @spec operation(map(), MapSet.t(), MapSet.t()) :: map()
  def operation(event_call, changes, wire_events) do
    {kind, opcode} =
      case event_call.action do
        :subscribe -> {:event_subscribe, @op_subscribe_events}
        :unsubscribe -> {:event_unsubscribe, @op_unsubscribe_events}
      end

    %{
      kind: kind,
      from: event_call.from,
      opcode: opcode,
      key: nil,
      payload: %{"events" => wire_payload(wire_events)},
      opts: event_call.opts,
      attempt: 0,
      original_reason: nil,
      event_call: event_call,
      event_changes: changes
    }
  end

  @spec restore_backoff(non_neg_integer()) :: pos_integer()
  def restore_backoff(attempt) do
    exponent = attempt |> max(1) |> min(6) |> Kernel.-(1)
    min(@restore_initial_backoff * :erlang.bsl(1, exponent), @restore_max_backoff)
  end

  defp wire_payload(events), do: EventSubscriptions.wire_payload(events)
end
