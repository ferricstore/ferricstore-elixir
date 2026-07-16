defmodule FerricStore.Test.ArchitectureCase do
  @moduledoc false

  import ExUnit.Assertions

  defmacro __using__(_opts) do
    quote do
      use ExUnit.Case, async: true

      import FerricStore.Test.ArchitectureCase
      alias FerricStore.Protocol.CommandSpec

      setup_all do
        {:ok, calls: ArchTest.Collector.calls(:ferricstore_sdk)}
      end
    end
  end

  def assert_no_calls(calls, opts) do
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

  def source_line_count(relative_path) do
    relative_path |> source() |> String.split("\n") |> length()
  end

  def source_contains?(relative_path, pattern),
    do: relative_path |> source() |> String.contains?(pattern)

  def source(relative_path), do: __DIR__ |> Path.join(relative_path) |> File.read!()

  def ferric_store_module?(module) do
    name = Atom.to_string(module)

    String.starts_with?(name, "Elixir.FerricStore") and
      not String.starts_with?(name, "Elixir.FerricStore.Test")
  end

  def topology_api_module?(module) do
    module in [
      FerricStore.SDK.KV,
      FerricStore.SDK.Flow,
      FerricStore.SDK.Management,
      FerricStore.SDK.Invocation,
      FerricStore.SDK.Admin
    ]
  end

  defp matches_module?(matcher, module) when is_function(matcher, 1), do: matcher.(module)
  defp matches_module?(modules, module) when is_list(modules), do: module in modules

  defp matches_function?(nil, _function), do: true
  defp matches_function?(functions, function), do: function in functions

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
