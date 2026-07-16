defmodule FerricStore.Flow.Options.ListReturnValidator do
  @moduledoc false

  def validate(:list, opts) do
    case Keyword.fetch(opts, :return) do
      :error -> :ok
      {:ok, value} -> if meta?(value), do: :ok, else: invalid()
    end
  end

  def validate(_operation, _opts), do: :ok

  defp meta?(value) when value in [nil, :meta], do: true

  defp meta?(<<m, e, t, a>>)
       when m in [?m, ?M] and e in [?e, ?E] and t in [?t, ?T] and a in [?a, ?A],
       do: true

  defp meta?(_value), do: false

  defp invalid,
    do: {:error, {:invalid_flow_option, :list, :return, :expected_meta_return}}
end
