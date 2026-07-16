defmodule FerricStore.Flow.PolicyValueValidatorTest do
  use ExUnit.Case, async: true

  alias FerricStore.Flow
  alias FerricStore.Flow.PolicyCommand

  test "rejects policy values outside the native server contract" do
    invalid = [
      {[max_active_ms: 0], "max_active_ms"},
      {[max_active_ms: 31_536_000_001], "max_active_ms"},
      {[indexed_attributes: "tenant"], "indexed_attributes"},
      {[indexed_attributes: ["a", "b", "c", "d"]], "indexed_attributes"},
      {[indexed_attributes: ["__internal"]], "indexed_attributes"},
      {[indexed_state_meta: ["a", "b"]], "indexed_state_meta"},
      {[retry: [max_retries: -1]], "retry.max_retries"},
      {[retry: [max_retries: 1_001]], "retry.max_retries"},
      {[retry: [backoff: :fixed]], "retry.backoff"},
      {[retry: [backoff: [kind: :random]]], "retry.backoff.kind"},
      {[retry: [backoff: [base_ms: -1]]], "retry.backoff.base_ms"},
      {[retry: [backoff: [max_ms: 2_592_000_001]]], "retry.backoff.max_ms"},
      {[retry: [backoff: [jitter_pct: 101]]], "retry.backoff.jitter_pct"},
      {[retry: [exhausted_to: ""]], "retry.exhausted_to"},
      {[retry: [exhausted_to: "running"]], "retry.exhausted_to"},
      {[retention: [ttl_ms: 0]], "retention.ttl_ms"},
      {[retention: [history_max_events: 1_000_001]], "retention.history_max_events"},
      {[states: %{"running" => %{mode: :fifo}}], "states"},
      {[states: %{"queued" => %{mode: :serial}}], "states.queued.mode"},
      {[states: %{"queued" => %{retry: %{max_retries: -1}}}], "states.queued.retry.max_retries"}
    ]

    for {opts, field} <- invalid do
      assert {:error, {:invalid_policy_option, ^field}} =
               PolicyCommand.set_payload("review", opts)
    end
  end

  test "bounded indexed-field validation rejects improper and oversized lists cheaply" do
    improper = ["tenant" | :invalid_tail]
    oversized = List.duplicate("tenant", 100_000)

    assert {:error, {:invalid_policy_option, "indexed_attributes"}} =
             PolicyCommand.set_payload("review", indexed_attributes: improper)

    for value <- [improper, oversized] do
      :erlang.garbage_collect(self())
      {:reductions, before_validation} = Process.info(self(), :reductions)

      assert {:error, {:invalid_policy_option, "indexed_attributes"}} =
               PolicyCommand.set_payload("review", indexed_attributes: value)

      {:reductions, after_validation} = Process.info(self(), :reductions)
      assert after_validation - before_validation < 10_000
    end
  end

  test "indexed-field validation rejects oversized and invalid UTF-8 keys before normalization" do
    oversized = String.duplicate("tenant", 200_000)

    for {option, field} <- [
          {:indexed_attributes, "indexed_attributes"},
          {:indexed_state_meta, "indexed_state_meta"}
        ] do
      {:reductions, before_validation} = Process.info(self(), :reductions)

      assert {:error, {:invalid_policy_option, ^field}} =
               PolicyCommand.set_payload("review", [{option, [oversized]}])

      {:reductions, after_validation} = Process.info(self(), :reductions)
      assert after_validation - before_validation < 10_000

      assert {:error, {:invalid_policy_option, ^field}} =
               PolicyCommand.set_payload("review", [{option, [<<"tenant", 0xFF>>]}])
    end
  end

  test "accepts values at the documented policy bounds" do
    assert {:ok, payload} =
             PolicyCommand.set_payload("review",
               max_active_ms: 31_536_000_000,
               indexed_attributes: ["tenant", :region],
               indexed_state_meta: ["version"],
               retry: [
                 max_retries: 1_000,
                 backoff: [
                   kind: :exponential,
                   base_ms: 0,
                   max_ms: 2_592_000_000,
                   jitter_pct: 100
                 ],
                 exhausted_to: "failed"
               ],
               retention: [ttl_ms: 31_536_000_000, history_max_events: 1_000_000],
               states: %{"queued" => %{mode: :fifo}}
             )

    assert payload["type"] == "review"
  end

  test "public policy payload helpers use the same validated contract as live calls" do
    opts = [retry: [max_retries: 3], states: %{"queued" => %{mode: :fifo}}]
    assert {:ok, expected} = PolicyCommand.set_payload("review", opts)
    assert Flow.policy_set_payload("review", opts) == expected

    assert {:error, {:invalid_policy_option, "retry.max_retries"}} =
             Flow.policy_set_payload("review", retry: [max_retries: -1])

    assert {:error, {:invalid_policy_option, "state"}} =
             Flow.policy_get_payload("review", state: 123)
  end
end
