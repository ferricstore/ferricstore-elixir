defmodule FerricStore.ArchitectureTest do
  use ExUnit.Case, async: true

  alias ArchTest.Collector

  setup_all do
    {:ok, calls: Collector.calls(:ferricstore_sdk)}
  end

  test "protocol codec stays below client and flow APIs", %{calls: calls} do
    assert_no_calls(calls,
      from: [FerricStore.Protocol],
      to: [FerricStore.Client, FerricStore.Flow, FerricStore.Queue, FerricStore.Workflow]
    )
  end

  test "socket client does not depend on high-level workflow modules", %{calls: calls} do
    assert_no_calls(calls,
      from: [FerricStore.Client],
      to: [FerricStore.Flow, FerricStore.Queue, FerricStore.Workflow]
    )
  end

  test "codec modules are transport independent", %{calls: calls} do
    assert_no_calls(calls,
      from: fn module ->
        module |> Atom.to_string() |> String.starts_with?("Elixir.FerricStore.Codec")
      end,
      to: [
        FerricStore.Client,
        FerricStore.Flow,
        FerricStore.Protocol,
        FerricStore.Queue,
        FerricStore.Workflow
      ]
    )
  end

  test "production modules do not contain debug IO calls", %{calls: calls} do
    assert_no_calls(calls,
      from: &ferric_store_module?/1,
      to: [IO],
      functions: [:puts, :inspect]
    )
  end

  test "production modules do not sleep in request paths", %{calls: calls} do
    assert_no_calls(calls,
      from: &ferric_store_module?/1,
      to: [Process],
      functions: [:sleep]
    )
  end

  defp assert_no_calls(calls, opts) do
    from = Keyword.fetch!(opts, :from)
    to = Keyword.fetch!(opts, :to)
    functions = Keyword.get(opts, :functions)

    violations =
      Enum.filter(calls, fn call ->
        matches_module?(from, call.caller_module) and matches_module?(to, call.callee_module) and
          matches_function?(functions, call.callee_function)
      end)

    assert violations == [], violation_message(violations)
  end

  defp matches_module?(matcher, module) when is_function(matcher, 1), do: matcher.(module)
  defp matches_module?(modules, module) when is_list(modules), do: module in modules

  defp matches_function?(nil, _function), do: true
  defp matches_function?(functions, function), do: function in functions

  defp ferric_store_module?(module) do
    module
    |> Atom.to_string()
    |> String.starts_with?("Elixir.FerricStore")
  end

  defp violation_message([]), do: ""

  defp violation_message(violations) do
    entries =
      Enum.map_join(violations, "\n", fn call ->
        location =
          case {call.file, call.line} do
            {nil, _line} -> "unknown"
            {file, nil} -> file
            {file, line} -> "#{file}:#{line}"
          end

        "#{inspect(call.caller_module)}.#{call.caller_function}/#{call.caller_arity} -> " <>
          "#{inspect(call.callee_module)}.#{call.callee_function}/#{call.callee_arity} at #{location}"
      end)

    "Forbidden architecture calls:\n#{entries}"
  end
end
