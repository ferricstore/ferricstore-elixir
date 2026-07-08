defmodule FerricStore.SDK.Management do
  @moduledoc """
  Narrow control-plane helpers for FerricStore management commands.

  These functions use the stable FerricStore management command contract over
  native `COMMAND_EXEC`. They intentionally expose named operations instead of
  requiring callers to hand-build management command strings.
  """

  alias FerricStore.SDK.Native.Client

  @type client :: GenServer.server()
  @type response :: {:ok, term()} | {:error, term()}

  @doc "Returns the server management capability map."
  @spec capabilities(client(), keyword()) :: response()
  def capabilities(client, opts \\ []), do: command(client, "FERRICSTORE.CAPABILITIES", [], opts)

  @doc "Creates or updates an ACL user with the given Redis-compatible ACL rules."
  @spec set_user(client(), binary(), [term()] | term(), keyword()) :: response()
  def set_user(client, username, rules, opts \\ []) when is_binary(username) do
    command(client, "ACL", ["SETUSER", username | normalize_rules(rules)], opts)
  end

  @doc "Deletes an ACL user."
  @spec del_user(client(), binary(), keyword()) :: response()
  def del_user(client, username, opts \\ []) when is_binary(username),
    do: command(client, "ACL", ["DELUSER", username], opts)

  @doc "Reads one ACL user."
  @spec get_user(client(), binary(), keyword()) :: response()
  def get_user(client, username, opts \\ []) when is_binary(username),
    do: command(client, "ACL", ["GETUSER", username], opts)

  @doc "Lists ACL users."
  @spec list_users(client(), keyword()) :: response()
  def list_users(client, opts \\ []), do: command(client, "ACL", ["LIST"], opts)

  @doc "Persists ACL state when the server supports ACL persistence."
  @spec save_acl(client(), keyword()) :: response()
  def save_acl(client, opts \\ []), do: command(client, "ACL", ["SAVE"], opts)

  @doc "Ensures namespace metadata exists for a prefix."
  @spec ensure_namespace(client(), binary(), map() | keyword(), keyword()) :: response()
  def ensure_namespace(client, prefix, attrs \\ %{}, opts \\ []) when is_binary(prefix) do
    command(client, "FERRICSTORE.NAMESPACE", ["ENSURE", prefix | pair_args(attrs)], opts)
  end

  @doc "Reads namespace metadata for a prefix."
  @spec get_namespace(client(), binary(), keyword()) :: response()
  def get_namespace(client, prefix, opts \\ []) when is_binary(prefix),
    do: command(client, "FERRICSTORE.NAMESPACE", ["GET", prefix], opts)

  @doc "Lists namespaces."
  @spec list_namespaces(client(), keyword()) :: response()
  def list_namespaces(client, opts \\ []),
    do: command(client, "FERRICSTORE.NAMESPACE", ["LIST"], opts)

  @doc "Deletes namespace metadata for a prefix."
  @spec delete_namespace(client(), binary(), keyword()) :: response()
  def delete_namespace(client, prefix, opts \\ []) when is_binary(prefix),
    do: command(client, "FERRICSTORE.NAMESPACE", ["DELETE", prefix], opts)

  @doc "Sets a quota specification for a namespace or scope."
  @spec set_quota(client(), binary(), map() | keyword(), keyword()) :: response()
  def set_quota(client, namespace, quota_spec, opts \\ []) when is_binary(namespace) do
    command(client, "FERRICSTORE.QUOTA", ["SET", namespace | pair_args(quota_spec)], opts)
  end

  @doc "Reads the quota specification for a namespace or scope."
  @spec get_quota(client(), binary(), keyword()) :: response()
  def get_quota(client, namespace, opts \\ []) when is_binary(namespace),
    do: command(client, "FERRICSTORE.QUOTA", ["GET", namespace], opts)

  @doc "Reads safe quota usage for a namespace or scope."
  @spec quota_usage(client(), binary(), keyword()) :: response()
  def quota_usage(client, namespace, opts \\ []) when is_binary(namespace),
    do: command(client, "FERRICSTORE.QUOTA", ["USAGE", namespace], opts)

  @doc "Reads safe cluster control-plane metadata."
  @spec cluster_info(client(), keyword()) :: response()
  def cluster_info(client, opts \\ []),
    do: command(client, "FERRICSTORE.TELEMETRY", ["CLUSTER_INFO"], opts)

  @doc "Reads safe namespace usage metadata."
  @spec namespace_usage(client(), binary(), keyword()) :: response()
  def namespace_usage(client, prefix, opts \\ []) when is_binary(prefix),
    do: command(client, "FERRICSTORE.TELEMETRY", ["NAMESPACE_USAGE", prefix], opts)

  @doc "Queries safe Flow observability metadata without payload search."
  @spec flow_query(client(), map() | keyword(), keyword()) :: response()
  def flow_query(client, attrs \\ %{}, opts \\ []) do
    command(client, "FERRICSTORE.TELEMETRY", ["FLOW_QUERY" | pair_args(attrs)], opts)
  end

  @doc "Reads safe Flow history metadata without payload search."
  @spec flow_history(client(), binary(), map() | keyword(), keyword()) :: response()
  def flow_history(client, id, attrs \\ %{}, opts \\ []) when is_binary(id) do
    command(client, "FERRICSTORE.TELEMETRY", ["FLOW_HISTORY", id | pair_args(attrs)], opts)
  end

  defp command(client, command, args, opts), do: Client.command_exec(client, command, args, opts)

  defp normalize_rules(rules) when is_list(rules), do: Enum.map(rules, &to_string/1)
  defp normalize_rules(rule), do: [to_string(rule)]

  defp pair_args(nil), do: []

  defp pair_args(%{} = pairs) do
    pairs
    |> Map.to_list()
    |> pair_args()
  end

  defp pair_args(pairs) when is_list(pairs) do
    Enum.flat_map(pairs, fn
      {_key, nil} ->
        []

      {key, value} ->
        [pair_key(key), pair_value(value)]
    end)
  end

  defp pair_key(key) when is_atom(key), do: key |> Atom.to_string() |> String.upcase()
  defp pair_key(key) when is_binary(key), do: String.upcase(key)
  defp pair_key(key), do: key |> to_string() |> String.upcase()

  defp pair_value(value) when is_boolean(value), do: value
  defp pair_value(value) when is_atom(value), do: Atom.to_string(value)
  defp pair_value(value), do: value
end
