defmodule FerricStore.Architecture.ProductionModuleSizeTest do
  use ExUnit.Case, async: true

  @max_lines 200

  test "production modules stay focused" do
    lib_root = Path.expand("../../../lib", __DIR__)

    oversized =
      lib_root
      |> Path.join("**/*.ex")
      |> Path.wildcard()
      |> Enum.map(&{Path.relative_to(&1, lib_root), line_count(&1)})
      |> Enum.filter(fn {_path, lines} -> lines > @max_lines end)
      |> Enum.sort_by(fn {_path, lines} -> -lines end)

    assert oversized == [],
           "production modules must not exceed #{@max_lines} lines: #{inspect(oversized)}"
  end

  defp line_count(path), do: path |> File.stream!() |> Enum.count()
end
