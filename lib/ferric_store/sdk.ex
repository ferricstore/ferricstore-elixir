defmodule FerricStore.SDK do
  @moduledoc """
  Topology-aware Elixir SDK entry points.

  Functions in this namespace accept clients returned by either `start_link/1`
  or `FerricStore.start_link/1`; both entry points create the same canonical
  topology-aware client.
  """

  alias FerricStore.SDK.Invocation
  alias FerricStore.SDK.KV
  alias FerricStore.SDK.Management
  alias FerricStore.SDK.Native.{Client, ClientOptions, Topology}

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) when is_list(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      restart: :permanent,
      shutdown: :infinity,
      type: :supervisor
    }
  end

  def start_link(opts) do
    with {:ok, {url, opts}} <- ClientOptions.take_url(opts) do
      case url do
        nil -> Client.start_link(opts)
        url -> Client.from_url(url, opts)
      end
    end
  end

  defdelegate from_url(url, opts \\ []), to: Client
  defdelegate ping(client, message \\ "PONG", opts \\ []), to: Client
  defdelegate command_exec(client, command, args \\ [], opts \\ []), to: Client
  defdelegate close(client), to: Client
  defdelegate close(client, timeout), to: Client
  defdelegate request(client, opcode, payload \\ %{}, opts \\ []), to: Client
  defdelegate request_by_key(client, opcode, key, payload, opts \\ []), to: Client
  defdelegate request_by_keys(client, opcode, keys, payload_builder, opts \\ []), to: Client

  defdelegate request_by_items(client, opcode, items, key_fun, payload_builder, opts \\ []),
    to: Client

  defdelegate refresh_topology(client, timeout \\ 5_000), to: Client
  defdelegate route(client, key), to: Client
  @doc "Returns the client's current read-only topology snapshot."
  @spec topology(pid()) :: Topology.t() | {:error, term()}
  defdelegate topology(client), to: Client
  defdelegate subscribe_events(client, events \\ [], opts \\ []), to: Client
  defdelegate unsubscribe_events(client, events \\ [], opts \\ []), to: Client
  defdelegate await_event(client, timeout \\ 5_000), to: Client

  def command(client, command, args \\ [], opts \\ []) do
    command_exec(client, command, args, opts)
  end

  defdelegate get(client, key, opts \\ []), to: KV
  defdelegate set(client, key, value, opts \\ []), to: KV
  defdelegate del(client, key_or_keys, opts \\ []), to: KV
  defdelegate mget(client, keys, opts \\ []), to: KV
  defdelegate mset(client, pairs, opts \\ []), to: KV
  defdelegate cas(client, key, expected, value, opts \\ []), to: KV
  defdelegate lock(client, key, owner, ttl_ms, opts \\ []), to: KV
  defdelegate unlock(client, key, owner, opts \\ []), to: KV
  defdelegate extend(client, key, owner, ttl_ms, opts \\ []), to: KV
  defdelegate ratelimit_add(client, key, window_ms, max, count \\ 1, opts \\ []), to: KV
  defdelegate fetch_or_compute(client, key, ttl_ms, opts \\ []), to: KV
  defdelegate fetch_or_compute_result(client, key, token, value, ttl_ms, opts \\ []), to: KV
  defdelegate fetch_or_compute_error(client, key, token, message, opts \\ []), to: KV
  defdelegate hset(client, key, fields, opts \\ []), to: KV
  defdelegate hget(client, key, field, opts \\ []), to: KV
  defdelegate hmget(client, key, fields, opts \\ []), to: KV
  defdelegate hgetall(client, key, opts \\ []), to: KV
  defdelegate lpush(client, key, values, opts \\ []), to: KV
  defdelegate rpush(client, key, values, opts \\ []), to: KV
  defdelegate lpop(client, key, count \\ 1, opts \\ []), to: KV
  defdelegate rpop(client, key, count \\ 1, opts \\ []), to: KV
  defdelegate lrange(client, key, start, stop, opts \\ []), to: KV
  defdelegate sadd(client, key, members, opts \\ []), to: KV
  defdelegate srem(client, key, members, opts \\ []), to: KV
  defdelegate smembers(client, key, opts \\ []), to: KV
  defdelegate sismember(client, key, member, opts \\ []), to: KV
  defdelegate zadd(client, key, items, opts \\ []), to: KV
  defdelegate zrem(client, key, members, opts \\ []), to: KV
  defdelegate zrange(client, key, start, stop, opts \\ []), to: KV
  defdelegate zscore(client, key, member, opts \\ []), to: KV

  defdelegate capabilities(client, opts \\ []), to: Management
  defdelegate acl_set_user(client, username, rules, opts \\ []), to: Management, as: :set_user
  defdelegate acl_del_user(client, username, opts \\ []), to: Management, as: :del_user
  defdelegate acl_get_user(client, username, opts \\ []), to: Management, as: :get_user
  defdelegate acl_list_users(client, opts \\ []), to: Management, as: :list_users
  defdelegate acl_save(client, opts \\ []), to: Management, as: :save_acl
  defdelegate ensure_namespace(client, prefix, attrs \\ %{}, opts \\ []), to: Management
  defdelegate get_namespace(client, prefix, opts \\ []), to: Management
  defdelegate list_namespaces(client, opts \\ []), to: Management
  defdelegate delete_namespace(client, prefix, opts \\ []), to: Management
  defdelegate set_quota(client, namespace, quota_spec, opts \\ []), to: Management
  defdelegate get_quota(client, namespace, opts \\ []), to: Management
  defdelegate quota_usage(client, namespace, opts \\ []), to: Management
  defdelegate cluster_info(client, opts \\ []), to: Management
  defdelegate namespace_usage(client, prefix, opts \\ []), to: Management
  defdelegate flow_query(client, attrs \\ %{}, opts \\ []), to: Management
  defdelegate flow_history(client, id, attrs \\ %{}, opts \\ []), to: Management

  defdelegate invocation_definition_put(client, definition, opts \\ []),
    to: Invocation,
    as: :put_definition

  defdelegate invocation_definition_get(client, name, opts \\ []),
    to: Invocation,
    as: :get_definition

  defdelegate invocation_definition_list(client, opts \\ []),
    to: Invocation,
    as: :list_definitions

  defdelegate invocation_create(client, name, attrs, opts \\ []), to: Invocation, as: :create
  defdelegate invocation_get(client, id, opts \\ []), to: Invocation, as: :get

  defdelegate invocation_partition_list(client, name, opts \\ []),
    to: Invocation,
    as: :list_partitions
end
