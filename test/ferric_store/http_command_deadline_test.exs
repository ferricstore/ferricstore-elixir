defmodule FerricStore.HTTPCommandDeadlineTest do
  use ExUnit.Case, async: true

  alias FerricStore.{DeadlineBudget, RequestContext, Timeout}
  alias FerricStore.HTTP.{CommandDeadline, Options}
  alias FerricStore.Protocol.{CommandSpec, PipelineRequest}

  @command_exec_opcode CommandSpec.fetch!(:command_exec).opcode
  @pipeline_opcode CommandSpec.fetch!(:pipeline).opcode

  test "finite blocking commands extend the implicit SDK deadline" do
    context = RequestContext.new([], 5_000)

    for {command, args, extension_ms} <- [
          {"BLPOP", ["queue", "45"], 45_000},
          {"BRPOP", ["queue", 1.5], 1_500},
          {"BLMOVE", ["source", "destination", "LEFT", "RIGHT", "2"], 2_000},
          {"BRPOPLPUSH", ["source", "destination", "3"], 3_000},
          {"BLMPOP", ["4", "1", "queue", "LEFT"], 4_000},
          {"BZPOPMIN", ["one", "two", "5"], 5_000},
          {"BZPOPMAX", ["one", "two", 6], 6_000},
          {"BZMPOP", ["7", "1", "queue", "MIN"], 7_000},
          {"XREAD", ["BLOCK", "8000", "STREAMS", "orders", "$"], 8_000},
          {"XREADGROUP",
           [
             "GROUP",
             "workers",
             "consumer",
             "COUNT",
             "1",
             "BLOCK",
             "9000",
             "STREAMS",
             "orders",
             ">"
           ], 9_000}
        ] do
      remaining =
        config(30_000)
        |> CommandDeadline.new(command_message(command, args, context), 6_000)
        |> DeadlineBudget.remaining()

      assert remaining in (extension_ms + 4_900)..(extension_ms + 5_000),
             "#{command} did not extend its implicit deadline"
    end
  end

  test "mixed pipelines add each sequential blocking wait to the implicit deadline" do
    context = RequestContext.new([], 5_000)

    commands = [
      ["PING"],
      ["BLPOP", "jobs", "1.5"],
      ["GET", "status"],
      ["XREAD", "COUNT", "1", "BLOCK", "2500", "STREAMS", "orders", "$"],
      ["BZPOPMIN", "scores", "3"]
    ]

    remaining =
      config(30_000)
      |> CommandDeadline.new(pipeline_message(commands, context), 6_000)
      |> DeadlineBudget.remaining()

    assert remaining in 11_900..12_000
  end

  test "zero blocking timeout disables only the implicit SDK deadline" do
    implicit = RequestContext.new([], 5_000)

    budget =
      CommandDeadline.new(
        config(30_000),
        command_message("BLPOP", ["jobs", "0"], implicit),
        6_000
      )

    assert DeadlineBudget.remaining(budget) == :infinity

    explicit = RequestContext.new([timeout: 50], 5_000)

    budget =
      CommandDeadline.new(
        config(30_000),
        command_message("XREAD", ["BLOCK", "0", "STREAMS", "orders", "$"], explicit),
        70
      )

    assert DeadlineBudget.remaining(budget) in 1..50
  end

  test "malformed blocking syntax cannot weaken the default deadline" do
    context = RequestContext.new([], 5_000)

    for {command, args} <- [
          {"BLPOP", ["jobs", "NaN"]},
          {"BLMPOP", ["not-a-timeout", "1", "jobs", "LEFT"]},
          {"XREAD", ["STREAMS", "BLOCK", "0"]},
          {"XREADGROUP", ["GROUP", "group", "consumer", "COUNT", "1", "STREAMS", "BLOCK", "0"]}
        ] do
      remaining =
        config(5_000)
        |> CommandDeadline.new(command_message(command, args, context), 6_000)
        |> DeadlineBudget.remaining()

      assert remaining in 1..5_000
    end
  end

  test "overflowing blocking pipeline budgets safely disable the implicit deadline" do
    context = RequestContext.new([], 5_000)
    huge_seconds = Integer.to_string(Timeout.max_finite())

    budget =
      CommandDeadline.new(
        config(30_000),
        pipeline_message(
          [["BLPOP", "one", huge_seconds], ["BRPOP", "two", huge_seconds]],
          context
        ),
        6_000
      )

    assert DeadlineBudget.remaining(budget) == :infinity
  end

  defp command_message(command, args, context) do
    {:request, @command_exec_opcode, %{"command" => command, "args" => args}, context}
  end

  defp pipeline_message(commands, context) do
    payload = %PipelineRequest{commands: commands, command_count: length(commands)}
    {:request, @pipeline_opcode, payload, context}
  end

  defp config(timeout) do
    %Options{
      base_url: "http://localhost",
      command_url: "http://localhost/v1/commands",
      headers: [],
      pool: __MODULE__,
      timeout: timeout
    }
  end
end
