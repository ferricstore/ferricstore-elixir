defmodule FerricStore.Protocol.RequestContextScopes do
  @moduledoc false

  alias FerricStore.Protocol.RequestContextScopeParser
  alias FerricStore.RequestLimits

  @max_scopes RequestLimits.max_batch_items()

  @spec normalize(term()) :: [binary()]
  def normalize(nil), do: []
  def normalize(scopes) when is_binary(scopes), do: RequestContextScopeParser.normalize(scopes)

  def normalize(scopes) when is_list(scopes),
    do: normalize_list(scopes, 0, MapSet.new(), [])

  def normalize(scopes) do
    raise ArgumentError,
          "request context field scopes must be a list or binary, got: #{inspect(scopes)}"
  end

  defp normalize_list([], _count, _seen, normalized), do: Enum.reverse(normalized)

  defp normalize_list([_scope | _scopes], @max_scopes, _seen, _normalized) do
    raise ArgumentError, "request context scopes exceed #{@max_scopes} items"
  end

  defp normalize_list([scope | scopes], count, seen, normalized) when is_binary(scope) do
    if MapSet.member?(seen, scope) do
      normalize_list(scopes, count + 1, seen, normalized)
    else
      normalize_list(scopes, count + 1, MapSet.put(seen, scope), [scope | normalized])
    end
  end

  defp normalize_list([_invalid | scopes], count, seen, normalized),
    do: normalize_list(scopes, count + 1, seen, normalized)

  defp normalize_list(improper_tail, _count, _seen, _normalized),
    do:
      raise(
        ArgumentError,
        "request context scopes must be a proper list, got: #{inspect(improper_tail)}"
      )
end
