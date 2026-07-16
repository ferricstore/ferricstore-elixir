defmodule FerricStore.OptionListTest do
  use ExUnit.Case, async: true

  alias FerricStore.RequestOptions
  alias FerricStore.SDK.Native.ClientOptions

  test "request option admission stops at its finite key budget" do
    opts = List.duplicate({:timeout, 10}, 100_000)

    {result, reductions} = measured(fn -> RequestOptions.validate(opts) end)

    assert {:error, {:options, {:too_many_options, %{limit: 32, observed: 33}}}} = result

    assert reductions < 20_000
  end

  test "native batch group concurrency has a finite resource ceiling" do
    assert :ok = RequestOptions.validate(max_group_concurrency: 256)

    assert {:error, {:max_group_concurrency, 257}} =
             RequestOptions.validate(max_group_concurrency: 257)
  end

  test "client option admission stops before traversing oversized keyword lists" do
    opts = List.duplicate({:tls, true}, 100_000)

    {result, reductions} = measured(fn -> ClientOptions.validate(opts) end)

    assert {:error, {:options, {:too_many_options, %{limit: limit, observed: observed}}}} = result

    assert observed == limit + 1
    assert reductions < 20_000
  end

  defp measured(fun) do
    :erlang.garbage_collect(self())
    {:reductions, before_reductions} = Process.info(self(), :reductions)
    result = fun.()
    {:reductions, after_reductions} = Process.info(self(), :reductions)
    {result, after_reductions - before_reductions}
  end
end
