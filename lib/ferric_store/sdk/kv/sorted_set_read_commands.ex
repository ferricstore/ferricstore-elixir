defmodule FerricStore.SDK.KV.SortedSetReadCommands do
  @moduledoc false

  alias FerricStore.Protocol.Opcodes
  alias FerricStore.RequestContext
  alias FerricStore.SDK.KV.{Input, Response}
  alias FerricStore.SDK.Native.KVRequests

  def zrange(client, key, start, stop, opts) do
    with {:ok, start} <- Input.integer(start, :zrange, :start),
         {:ok, stop} <- Input.integer(stop, :zrange, :stop) do
      withscores = RequestContext.option(opts, :withscores)

      payload =
        %{"key" => key, "start" => start, "stop" => stop}
        |> maybe_put("withscores", withscores)

      client
      |> KVRequests.request_by_key(Opcodes.zrange(), key, payload, opts)
      |> Response.zrange(withscores == true, RequestContext.budget(opts))
    end
  end

  def zscore(client, key, member, opts) do
    with {:ok, member} <- Input.binary(member, :zscore, :member) do
      client
      |> KVRequests.request_by_key(
        Opcodes.zscore(),
        key,
        %{"key" => key, "member" => member},
        opts
      )
      |> Response.score(:zscore)
    end
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
