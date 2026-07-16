defmodule FerricStore.SDK.Native.RetryPolicy do
  @moduledoc false

  alias FerricStore.Protocol.Opcodes
  alias FerricStore.RequestContext

  @spec retryable?(term(), non_neg_integer(), RequestContext.t()) :: boolean()
  def retryable?({:connect_failed, _reason}, _opcode, %RequestContext{}), do: true
  def retryable?({:reroute, _payload}, _opcode, %RequestContext{}), do: true
  def retryable?(:connection_draining, _opcode, %RequestContext{}), do: true

  def retryable?(:connection_drained, opcode, %RequestContext{} = context),
    do: replay_safe?(opcode, context)

  def retryable?({failure, _reason}, opcode, %RequestContext{} = context)
      when failure in [:send_failed, :transport_failed] do
    replay_safe?(opcode, context)
  end

  def retryable?(_reason, _opcode, %RequestContext{}), do: false

  defp replay_safe?(opcode, context),
    do: RequestContext.option(context, :idempotent, false) or Opcodes.read_only?(opcode)
end
