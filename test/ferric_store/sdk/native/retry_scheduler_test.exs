defmodule FerricStore.SDK.Native.RetrySchedulerTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.RetryScheduler

  test "delays request and batch retries by the bounded server hint" do
    request_tag = make_ref()
    batch_id = make_ref()
    reason = {:busy, %{"retry_after_ms" => 20}}

    assert :waiting = RetryScheduler.request(request_tag, reason)
    assert :waiting = RetryScheduler.batch(batch_id, reason)
    refute_received {:retry_request, ^request_tag}
    refute_received {:retry_batch, ^batch_id}
    assert_receive {:retry_request, ^request_tag}, 100
    assert_receive {:retry_batch, ^batch_id}, 100
  end

  test "zero-delay retries remain synchronous" do
    assert :ready = RetryScheduler.request(make_ref(), {:reroute, %{"retry_after_ms" => 0}})
    assert :ready = RetryScheduler.batch(make_ref(), :connection_draining)
  end
end
