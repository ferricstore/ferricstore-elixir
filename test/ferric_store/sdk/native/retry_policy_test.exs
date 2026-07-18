defmodule FerricStore.SDK.Native.RetryPolicyTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol.Opcodes
  alias FerricStore.RequestContext
  alias FerricStore.SDK.Native.RetryPolicy

  test "only retries send ambiguity for reads or explicitly idempotent operations" do
    empty = context()
    idempotent = context(idempotent: true)

    assert RetryPolicy.retryable?({:connect_failed, :econnrefused}, 0x0102, empty)

    assert RetryPolicy.retryable?(
             retry_error(:reroute, retry_after_ms: 0),
             0x0102,
             empty
           )

    assert RetryPolicy.retryable?({:send_failed, :closed}, 0x0101, empty)
    assert RetryPolicy.retryable?({:transport_failed, :closed}, 0x0101, empty)

    refute RetryPolicy.retryable?({:send_failed, :closed}, 0x0102, empty)
    refute RetryPolicy.retryable?({:send_failed, :closed}, 0x0102, idempotent)
    refute RetryPolicy.retryable?({:transport_failed, :closed}, 0x0102, empty)
    refute RetryPolicy.retryable?({:transport_failed, :econnreset}, 0x0102, idempotent)
    refute RetryPolicy.retryable?({:send_failed, :closed}, 0x0120, empty)
    refute RetryPolicy.retryable?(:timeout, 0x0101, empty)
  end

  test "follows structured server retry safety and delay metadata" do
    context = context()

    for status <- [:busy, :reroute] do
      reason = retry_error(status, retry_after_ms: 275)

      assert RetryPolicy.retryable?(reason, Opcodes.set(), context)
      assert RetryPolicy.retry_after_ms(reason) == 275

      refute RetryPolicy.retryable?(
               retry_error(status, retryable: false),
               Opcodes.set(),
               context
             )

      refute RetryPolicy.retryable?(
               retry_error(status, safe_to_retry: false),
               Opcodes.set(),
               context
             )
    end

    refute RetryPolicy.retryable?({:reroute, %{}}, Opcodes.get(), context)
    refute RetryPolicy.retryable?({:busy, "ERR overloaded"}, Opcodes.get(), context)
    assert RetryPolicy.retry_after_ms({:busy, %{"retry_after_ms" => -1}}) == 0

    assert RetryPolicy.retry_after_ms(
             {:group_failures,
              [
                retry_error(:busy, retry_after_ms: 25),
                retry_error(:reroute, retry_after_ms: 75)
              ]}
           ) == 75
  end

  test "retries requests rejected by a draining connection without duplicating ambiguous writes" do
    empty = context()

    assert RetryPolicy.retryable?(:connection_draining, Opcodes.set(), empty)
    assert RetryPolicy.retryable?(:connection_drained, Opcodes.get(), empty)

    refute RetryPolicy.retryable?(:connection_drained, Opcodes.set(), empty)
    refute RetryPolicy.retryable?(:connection_drained, Opcodes.set(), context(idempotent: true))
  end

  test "uses the complete server-provided retry delay" do
    reason = retry_error(:busy, retry_after_ms: 120_000)

    assert RetryPolicy.retryable?(reason, Opcodes.set(), context())
    assert RetryPolicy.retry_after_ms(reason) == 120_000
  end

  test "read retry metadata covers current flow and cluster query opcodes" do
    read_opcodes = [
      Opcodes.flow_effect_get(),
      Opcodes.flow_approval_get(),
      Opcodes.flow_budget_get(),
      Opcodes.flow_limit_get(),
      Opcodes.flow_approval_list(),
      Opcodes.flow_governance_overview(),
      Opcodes.cluster_health(),
      Opcodes.cluster_stats(),
      Opcodes.cluster_slots(),
      Opcodes.ferricstore_metrics()
    ]

    Enum.each(read_opcodes, fn opcode ->
      assert Opcodes.read_only?(opcode)
      assert RetryPolicy.retryable?({:send_failed, :closed}, opcode, context())
    end)

    refute Opcodes.read_only?(Opcodes.set())
    refute Opcodes.read_only?(Opcodes.flow_budget_commit())
    refute Opcodes.read_only?(Opcodes.cluster_join())
  end

  test "rejects the removed raw keyword-list retry contract" do
    assert_raise FunctionClauseError, fn ->
      :erlang.apply(RetryPolicy, :retryable?, [{:connect_failed, :closed}, Opcodes.get(), []])
    end
  end

  defp context(options \\ []), do: RequestContext.new(options, 100)

  defp retry_error(status, overrides) do
    fields =
      %{
        "retryable" => Keyword.get(overrides, :retryable, true),
        "safe_to_retry" => Keyword.get(overrides, :safe_to_retry, true),
        "retry_after_ms" => Keyword.get(overrides, :retry_after_ms, 0)
      }

    {status, fields}
  end
end
