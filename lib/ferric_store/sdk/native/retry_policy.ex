defmodule FerricStore.SDK.Native.RetryPolicy do
  @moduledoc false

  alias FerricStore.Protocol.Opcodes
  alias FerricStore.RequestContext
  alias FerricStore.Types

  @spec retryable?(term(), non_neg_integer(), RequestContext.t()) :: boolean()
  def retryable?({:connect_failed, _reason}, _opcode, %RequestContext{}), do: true

  def retryable?({status, payload}, _opcode, %RequestContext{})
      when status in [:busy, :reroute] and is_map(payload),
      do:
        Types.get(payload, "retryable") == true and
          Types.get(payload, "safe_to_retry") == true

  def retryable?(:connection_draining, _opcode, %RequestContext{}), do: true

  def retryable?(:connection_drained, opcode, %RequestContext{}),
    do: Opcodes.read_only?(opcode)

  def retryable?({failure, _reason}, opcode, %RequestContext{})
      when failure in [:send_failed, :transport_failed] do
    Opcodes.read_only?(opcode)
  end

  def retryable?(_reason, _opcode, %RequestContext{}), do: false

  @spec retry_after_ms(term()) :: non_neg_integer()
  def retry_after_ms({status, payload})
      when status in [:busy, :reroute] and is_map(payload) do
    case Types.get(payload, "retry_after_ms") do
      delay when is_integer(delay) and delay >= 0 -> delay
      _invalid_or_missing -> 0
    end
  end

  def retry_after_ms({:group_failures, reasons}) when is_list(reasons),
    do: Enum.reduce(reasons, 0, &max(retry_after_ms(&1), &2))

  def retry_after_ms(_reason), do: 0
end
