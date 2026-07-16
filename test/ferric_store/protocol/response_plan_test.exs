defmodule FerricStore.Protocol.ResponsePlanTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol.{CommandSpec, ResponsePlan}

  test "derives compact claim layouts from normalized typed pipeline commands" do
    claim_opcode = CommandSpec.fetch!(:flow_claim_due).opcode
    get_opcode = CommandSpec.fetch!(:get).opcode

    payload = %{
      "commands" => [
        %{"opcode" => claim_opcode, "body" => %{"return" => "JOBS_COMPACT"}},
        %{"opcode" => get_opcode, "body" => %{"key" => "key"}},
        %{"opcode" => claim_opcode, "body" => %{"return" => "JOBS_COMPACT_STATE_ATTRS"}}
      ]
    }

    assert ResponsePlan.build(CommandSpec.fetch!(:pipeline).opcode, payload) ==
             [:base, :unknown, :state_attrs]
  end

  test "derives compact claim layouts from normalized raw commands" do
    command_opcode = CommandSpec.fetch!(:command_exec).opcode
    pipeline_opcode = CommandSpec.fetch!(:pipeline).opcode

    payload = %{
      commands: [
        %{
          opcode: command_opcode,
          body: %{"command" => "FLOW.CLAIM_DUE", "args" => ["RETURN", "jobs_compact_attrs"]}
        }
      ]
    }

    assert ResponsePlan.build(pipeline_opcode, payload) == [:attrs]
  end

  test "does not build plans for other opcodes or malformed payloads" do
    assert ResponsePlan.build(CommandSpec.fetch!(:get).opcode, %{}) == nil
    assert ResponsePlan.build(CommandSpec.fetch!(:pipeline).opcode, %{}) == nil
  end

  test "derives a direct compact claim layout from its request" do
    claim_opcode = CommandSpec.fetch!(:flow_claim_due).opcode
    command_opcode = CommandSpec.fetch!(:command_exec).opcode

    assert ResponsePlan.build(claim_opcode, %{"return" => "JOBS_COMPACT_STATE"}) ==
             :state

    assert ResponsePlan.build(command_opcode, %{
             "command" => "FLOW.CLAIM_DUE",
             "args" => ["TYPE", "email", "RETURN", "jobs_compact_attrs"]
           }) == :attrs
  end

  test "recognizes every compact claim return alias accepted by the server" do
    claim_opcode = CommandSpec.fetch!(:flow_claim_due).opcode

    aliases = %{
      base: ~w(JOBS_COMPACT JOB_COMPACT),
      attrs:
        ~w(JOBS_COMPACT_ATTRS JOB_COMPACT_ATTRS JOBS_COMPACT_ATTRIBUTES JOB_COMPACT_ATTRIBUTES),
      state:
        ~w(JOBS_COMPACT_STATE JOB_COMPACT_STATE JOBS_COMPACT_WITH_STATE JOB_COMPACT_WITH_STATE),
      state_attrs:
        ~w(JOBS_COMPACT_STATE_ATTRS JOB_COMPACT_STATE_ATTRS JOBS_COMPACT_WITH_STATE_ATTRS JOB_COMPACT_WITH_STATE_ATTRS JOBS_COMPACT_STATE_ATTRIBUTES JOB_COMPACT_STATE_ATTRIBUTES JOBS_COMPACT_WITH_STATE_ATTRIBUTES JOB_COMPACT_WITH_STATE_ATTRIBUTES)
    }

    for {mode, names} <- aliases, name <- names do
      assert ResponsePlan.build(claim_opcode, %{"return" => name}) == mode
    end
  end
end
