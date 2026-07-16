defmodule FerricStore.SDK.Native.CoordinatorRequest do
  @moduledoc false

  alias FerricStore.Protocol.CommandSpec
  alias FerricStore.RequestContext

  @spec default_lane_id(non_neg_integer()) :: 0 | 1
  def default_lane_id(opcode), do: if(CommandSpec.control_lane?(opcode), do: 0, else: 1)

  @spec control(GenServer.from(), non_neg_integer(), term(), RequestContext.t()) :: map()
  def control(from, opcode, payload, %RequestContext{} = context) do
    base(:control, from, opcode, nil, payload, context)
  end

  @spec routed(GenServer.from(), non_neg_integer(), binary(), term(), RequestContext.t()) :: map()
  def routed(from, opcode, key, payload, %RequestContext{} = context) do
    base(:routed, from, opcode, key, payload, context)
  end

  @spec registered(map(), reference(), non_neg_integer(), reference() | nil, reference() | nil) ::
          map()
  def registered(request, tag, lane_id, timer, caller_monitor) do
    Map.merge(request, %{
      tag: tag,
      conn: nil,
      lane_id: lane_id,
      timer: timer,
      caller_monitor: caller_monitor
    })
  end

  @spec caller_monitor(map()) :: reference() | nil
  def caller_monitor(%{kind: kind, from: {caller, _tag}})
      when kind in [:control, :routed],
      do: Process.monitor(caller)

  def caller_monitor(%{kind: kind, from: {:async, caller, _ref}})
      when kind in [:control, :routed],
      do: Process.monitor(caller)

  def caller_monitor(_request), do: nil

  @spec batch_group(
          reference(),
          map(),
          reference(),
          reference() | nil,
          RequestContext.t()
        ) :: map()
  def batch_group(batch_id, group, tag, timer, %RequestContext{} = context) do
    %{
      kind: :batch_group,
      batch_id: batch_id,
      group: group,
      conn: group.conn,
      opts: context,
      timer: timer,
      tag: tag
    }
  end

  defp base(kind, from, opcode, key, payload, context) do
    %{
      kind: kind,
      from: from,
      opcode: opcode,
      key: key,
      payload: payload,
      opts: context,
      attempt: 0,
      original_reason: nil
    }
  end
end
