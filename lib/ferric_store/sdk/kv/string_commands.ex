defmodule FerricStore.SDK.KV.StringCommands do
  @moduledoc false

  alias FerricStore.Protocol.Opcodes
  alias FerricStore.RequestContext
  alias FerricStore.SDK.KV.{Input, Response}
  alias FerricStore.SDK.Native.KVRequests

  def get(client, key, opts) do
    client
    |> KVRequests.request_by_key(Opcodes.get(), key, %{"key" => key}, opts)
    |> Response.binary_or_nil(:get)
  end

  def set(client, key, value, opts) do
    with {:ok, value} <- Input.binary(value, :set, :value) do
      payload =
        %{"key" => key, "value" => value}
        |> maybe_put("ttl", RequestContext.option(opts, :ttl))
        |> maybe_put("nx", RequestContext.option(opts, :nx))
        |> maybe_put("xx", RequestContext.option(opts, :xx))
        |> maybe_put("get", RequestContext.option(opts, :get))
        |> maybe_put("keepttl", RequestContext.option(opts, :keepttl))
        |> maybe_put("exat", RequestContext.option(opts, :exat))
        |> maybe_put("pxat", RequestContext.option(opts, :pxat))

      client
      |> KVRequests.request_by_key(Opcodes.set(), key, payload, opts)
      |> Response.set(
        RequestContext.option(opts, :get, false) == true,
        RequestContext.option(opts, :nx, false) == true
      )
    end
  end

  def cas(client, key, expected, value, opts) do
    with {:ok, expected} <- Input.binary(expected, :cas, :expected),
         {:ok, value} <- Input.binary(value, :cas, :value) do
      payload =
        %{"key" => key, "expected" => expected, "value" => value}
        |> maybe_put("ttl", RequestContext.option(opts, :ttl))

      client
      |> KVRequests.request_by_key(Opcodes.cas(), key, payload, opts)
      |> Response.boolean_or_nil(:cas)
    end
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
