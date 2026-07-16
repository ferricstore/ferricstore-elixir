defmodule FerricStore.Flow.ValueCommands do
  @moduledoc false

  alias FerricStore.Codec.Raw

  alias FerricStore.Flow.{
    CodecRuntime,
    CommandRuntime,
    RequestRuntime,
    Response,
    ValueRefsValidator
  }

  alias FerricStore.{Protocol, RequestContext, Result}

  @spec put(pid(), term(), keyword()) :: term()
  def put(client, value, opts) do
    CommandRuntime.with_options(:value_put, opts, fn opts, context ->
      codec = Keyword.get(opts, :codec, Raw)

      case CodecRuntime.encode(codec, value, RequestContext.budget(context)) do
        {:ok, encoded} -> request_put(client, encoded, opts, context)
        {:error, :timeout} -> Result.error(:timeout)
      end
    end)
  end

  @spec mget(pid(), term(), keyword()) :: term()
  def mget(_client, [], opts), do: CommandRuntime.empty_batch(:value_mget, opts)

  def mget(client, refs, opts) when is_list(refs) do
    CommandRuntime.with_options(:value_mget, opts, fn opts, context ->
      case ValueRefsValidator.validate(refs, RequestContext.budget(context)) do
        :ok -> request_mget(client, refs, opts, context)
        {:error, reason} -> Result.error(reason)
      end
    end)
  end

  def mget(_client, _refs, _opts),
    do: Result.error({:invalid_flow_value_refs, :expected_list})

  defp request_put(client, encoded, opts, context) do
    payload =
      %{
        "value" => encoded,
        "now_ms" => Keyword.get(opts, :now_ms, System.system_time(:millisecond))
      }
      |> put_if_present("partition_key", Keyword.get(opts, :partition_key))
      |> put_if_present("owner_flow_id", Keyword.get(opts, :owner_flow_id))
      |> put_if_present("name", Keyword.get(opts, :name))
      |> put_if_present("override", Keyword.get(opts, :override))
      |> put_if_present("ttl_ms", Keyword.get(opts, :ttl_ms))
      |> put_if_present("local_cache", Keyword.get(opts, :local_cache))

    RequestRuntime.request(client, Protocol.opcode(:flow_value_put), payload, opts, context)
  end

  defp request_mget(client, refs, opts, context) do
    payload =
      %{"refs" => refs}
      |> put_if_present("max_bytes", Keyword.get(opts, :max_bytes))
      |> put_if_present("value_max_bytes", Keyword.get(opts, :value_max_bytes))
      |> put_if_present("payload_max_bytes", Keyword.get(opts, :payload_max_bytes))

    client
    |> RequestRuntime.request(Protocol.opcode(:flow_value_mget), payload, opts, context)
    |> Response.decode_values(opts, refs, context)
  end

  defp put_if_present(map, _key, nil), do: map
  defp put_if_present(map, key, value), do: Map.put(map, key, value)
end
