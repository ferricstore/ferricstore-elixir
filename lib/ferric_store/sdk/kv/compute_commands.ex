defmodule FerricStore.SDK.KV.ComputeCommands do
  @moduledoc false

  alias FerricStore.Protocol.Opcodes
  alias FerricStore.RequestContext
  alias FerricStore.SDK.KV.{Input, Response, StructuredResponse}
  alias FerricStore.SDK.Native.KVRequests

  def ratelimit_add(client, key, window_ms, max, count, opts) do
    with {:ok, window_ms} <- Input.positive_integer(window_ms, :ratelimit_add, :window_ms),
         {:ok, max} <- Input.positive_integer(max, :ratelimit_add, :max),
         {:ok, count} <- Input.positive_integer(count, :ratelimit_add, :count) do
      client
      |> KVRequests.request_by_key(
        Opcodes.ratelimit_add(),
        key,
        %{"key" => key, "window_ms" => window_ms, "max" => max, "count" => count},
        opts
      )
      |> StructuredResponse.rate_limit(max, window_ms, count)
    end
  end

  def fetch_or_compute(client, key, ttl_ms, opts) do
    with {:ok, ttl_ms} <- Input.positive_integer(ttl_ms, :fetch_or_compute, :ttl_ms) do
      payload =
        %{"key" => key, "ttl_ms" => ttl_ms}
        |> maybe_put("hint", RequestContext.option(opts, :hint))

      client
      |> KVRequests.request_by_key(Opcodes.fetch_or_compute(), key, payload, opts)
      |> StructuredResponse.fetch_or_compute()
    end
  end

  def fetch_or_compute_result(client, key, token, value, ttl_ms, opts) do
    with {:ok, token} <- Input.nonempty_binary(token, :fetch_or_compute_result, :token),
         {:ok, value} <- Input.binary(value, :fetch_or_compute_result, :value),
         {:ok, ttl_ms} <- Input.positive_integer(ttl_ms, :fetch_or_compute_result, :ttl_ms) do
      client
      |> KVRequests.request_by_key(
        Opcodes.fetch_or_compute_result(),
        key,
        %{"key" => key, "token" => token, "value" => value, "ttl_ms" => ttl_ms},
        opts
      )
      |> Response.ok(:fetch_or_compute_result)
    end
  end

  def fetch_or_compute_error(client, key, token, message, opts) do
    with {:ok, token} <- Input.nonempty_binary(token, :fetch_or_compute_error, :token),
         {:ok, message} <- Input.binary(message, :fetch_or_compute_error, :message) do
      client
      |> KVRequests.request_by_key(
        Opcodes.fetch_or_compute_error(),
        key,
        %{"key" => key, "token" => token, "message" => message},
        opts
      )
      |> Response.ok(:fetch_or_compute_error)
    end
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)
end
