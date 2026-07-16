defmodule FerricStore.Protocol.RequestContextScopeParser do
  @moduledoc false

  alias FerricStore.RequestLimits

  @max_scopes RequestLimits.max_batch_items()

  @spec normalize(binary()) :: [binary()]
  def normalize(scopes) when is_binary(scopes) do
    pattern = :binary.compile_pattern([",", " "])
    parse(scopes, pattern, 0, MapSet.new(), [])
  end

  defp parse("", _pattern, _count, _seen, normalized), do: Enum.reverse(normalized)

  defp parse(scopes, pattern, count, seen, normalized) do
    case :binary.split(scopes, pattern) do
      [scope] -> add_scope(scope, "", pattern, count, seen, normalized)
      ["", rest] -> parse(rest, pattern, count, seen, normalized)
      [scope, rest] -> add_scope(scope, rest, pattern, count, seen, normalized)
    end
  end

  defp add_scope(_scope, _rest, _pattern, @max_scopes, _seen, _normalized) do
    raise ArgumentError, "request context scopes exceed #{@max_scopes} items"
  end

  defp add_scope(scope, rest, pattern, count, seen, normalized) do
    if MapSet.member?(seen, scope) do
      parse(rest, pattern, count + 1, seen, normalized)
    else
      scope = :binary.copy(scope)
      parse(rest, pattern, count + 1, MapSet.put(seen, scope), [scope | normalized])
    end
  end
end
