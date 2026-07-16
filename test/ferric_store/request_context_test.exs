defmodule FerricStore.RequestContextTest do
  use ExUnit.Case, async: true

  alias FerricStore.RequestContext
  alias FerricStore.Timeout

  test "internal option access accepts only a typed request context" do
    context = RequestContext.new([idempotent: true], 100)

    assert RequestContext.option(context, :idempotent) == true
    assert RequestContext.options(context) == [idempotent: true]

    assert_raise FunctionClauseError, fn ->
      :erlang.apply(RequestContext, :option, [[idempotent: true], :idempotent])
    end

    assert_raise FunctionClauseError, fn ->
      :erlang.apply(RequestContext, :options, [[idempotent: true]])
    end
  end

  test "coordinator calls cannot outlive the request's absolute deadline" do
    context = RequestContext.new([timeout: 0], 5_000)

    assert RequestContext.call_timeout(context, 5_000) == 0
    assert RequestContext.ensure_active(context) == {:error, :timeout}
  end

  test "coordinator call timeout leaves a reply margin after the request deadline" do
    context = RequestContext.new([timeout: 100], 5_000)
    timeout = RequestContext.call_timeout(context, 5_000)

    assert timeout >= 1_090
    assert timeout <= 1_100
  end

  test "explicit call timeout preserves its cleanup margin" do
    context = RequestContext.new([timeout: 1_000, call_timeout: 60], 5_000)
    timeout = RequestContext.call_timeout(context, 5_000)

    assert timeout >= 50
    assert timeout <= 60
  end

  test "very short explicit call timeouts are not extended" do
    context = RequestContext.new([timeout: 1_000, call_timeout: 10], 5_000)

    assert RequestContext.call_timeout(context, 5_000) in 0..10
  end

  test "finite timeout margins saturate instead of becoming unbounded" do
    max_finite = Timeout.max_finite()

    assert Timeout.add_margin(max_finite, 1_000) == max_finite

    context = RequestContext.new([timeout: max_finite], 5_000)
    assert RequestContext.call_timeout(context, 5_000) <= max_finite
  end
end
