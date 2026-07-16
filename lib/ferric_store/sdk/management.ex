defmodule FerricStore.SDK.Management do
  @moduledoc """
  Narrow control-plane helpers for FerricStore management commands.

  These functions use the stable FerricStore management command contract over
  native `COMMAND_EXEC`. They intentionally expose named operations instead of
  requiring callers to hand-build management command strings.
  """

  alias FerricStore.RequestContext
  alias FerricStore.SDK.ManagementInput
  alias FerricStore.SDK.Native.PreparedRequests

  @type client :: pid()
  @type response :: {:ok, term()} | {:error, term()}

  @doc "Returns the server management capability map."
  @spec capabilities(client(), keyword()) :: response()
  def capabilities(client, opts \\ []) do
    with {:ok, context} <- request_context(opts, :read),
         do: command(client, "FERRICSTORE.CAPABILITIES", [], context)
  end

  @doc "Creates or updates an ACL user with the given Redis-compatible ACL rules."
  @spec set_user(client(), binary(), [term()] | term(), keyword()) :: response()
  def set_user(client, username, rules, opts \\ []) do
    with {:ok, context} <- request_context(opts, :write),
         {:ok, username} <- ManagementInput.nonempty_binary(username, :set_user, :username),
         {:ok, rules} <- ManagementInput.normalize_rules(rules, RequestContext.budget(context)) do
      command(client, "ACL", ["SETUSER", username | rules], context)
    end
  end

  @doc "Deletes an ACL user."
  @spec del_user(client(), binary(), keyword()) :: response()
  def del_user(client, username, opts \\ []) do
    with {:ok, context} <- request_context(opts, :write),
         {:ok, username} <- ManagementInput.nonempty_binary(username, :del_user, :username),
         do: command(client, "ACL", ["DELUSER", username], context)
  end

  @doc "Reads one ACL user."
  @spec get_user(client(), binary(), keyword()) :: response()
  def get_user(client, username, opts \\ []) do
    with {:ok, context} <- request_context(opts, :read),
         {:ok, username} <- ManagementInput.nonempty_binary(username, :get_user, :username),
         do: command(client, "ACL", ["GETUSER", username], context)
  end

  @doc "Lists ACL users."
  @spec list_users(client(), keyword()) :: response()
  def list_users(client, opts \\ []) do
    with {:ok, context} <- request_context(opts, :read),
         do: command(client, "ACL", ["LIST"], context)
  end

  @doc "Persists ACL state when the server supports ACL persistence."
  @spec save_acl(client(), keyword()) :: response()
  def save_acl(client, opts \\ []) do
    with {:ok, context} <- request_context(opts, :write),
         do: command(client, "ACL", ["SAVE"], context)
  end

  @doc "Ensures namespace metadata exists for a prefix."
  @spec ensure_namespace(client(), binary(), term(), keyword()) :: response()
  def ensure_namespace(client, prefix, attrs \\ %{}, opts \\ []) do
    with {:ok, context} <- request_context(opts, :write),
         {:ok, prefix} <- ManagementInput.nonempty_binary(prefix, :ensure_namespace, :prefix),
         {:ok, args} <-
           ManagementInput.pair_args(
             attrs,
             :ensure_namespace,
             :attrs,
             RequestContext.budget(context)
           ) do
      command(client, "FERRICSTORE.NAMESPACE", ["ENSURE", prefix | args], context)
    end
  end

  @doc "Reads namespace metadata for a prefix."
  @spec get_namespace(client(), binary(), keyword()) :: response()
  def get_namespace(client, prefix, opts \\ []) do
    with {:ok, context} <- request_context(opts, :read),
         {:ok, prefix} <- ManagementInput.nonempty_binary(prefix, :get_namespace, :prefix),
         do: command(client, "FERRICSTORE.NAMESPACE", ["GET", prefix], context)
  end

  @doc "Lists namespaces."
  @spec list_namespaces(client(), keyword()) :: response()
  def list_namespaces(client, opts \\ []) do
    with {:ok, context} <- request_context(opts, :read),
         do: command(client, "FERRICSTORE.NAMESPACE", ["LIST"], context)
  end

  @doc "Deletes namespace metadata for a prefix."
  @spec delete_namespace(client(), binary(), keyword()) :: response()
  def delete_namespace(client, prefix, opts \\ []) do
    with {:ok, context} <- request_context(opts, :write),
         {:ok, prefix} <- ManagementInput.nonempty_binary(prefix, :delete_namespace, :prefix),
         do: command(client, "FERRICSTORE.NAMESPACE", ["DELETE", prefix], context)
  end

  @doc "Sets a quota specification for a namespace or scope."
  @spec set_quota(client(), binary(), term(), keyword()) :: response()
  def set_quota(client, namespace, quota_spec, opts \\ []) do
    with {:ok, context} <- request_context(opts, :write),
         {:ok, namespace} <- ManagementInput.nonempty_binary(namespace, :set_quota, :namespace),
         {:ok, args} <-
           ManagementInput.pair_args(
             quota_spec,
             :set_quota,
             :quota_spec,
             RequestContext.budget(context)
           ) do
      command(client, "FERRICSTORE.QUOTA", ["SET", namespace | args], context)
    end
  end

  @doc "Reads the quota specification for a namespace or scope."
  @spec get_quota(client(), binary(), keyword()) :: response()
  def get_quota(client, namespace, opts \\ []) do
    with {:ok, context} <- request_context(opts, :read),
         {:ok, namespace} <- ManagementInput.nonempty_binary(namespace, :get_quota, :namespace),
         do: command(client, "FERRICSTORE.QUOTA", ["GET", namespace], context)
  end

  @doc "Reads safe quota usage for a namespace or scope."
  @spec quota_usage(client(), binary(), keyword()) :: response()
  def quota_usage(client, namespace, opts \\ []) do
    with {:ok, context} <- request_context(opts, :read),
         {:ok, namespace} <- ManagementInput.nonempty_binary(namespace, :quota_usage, :namespace),
         do: command(client, "FERRICSTORE.QUOTA", ["USAGE", namespace], context)
  end

  @doc "Reads safe cluster control-plane metadata."
  @spec cluster_info(client(), keyword()) :: response()
  def cluster_info(client, opts \\ []) do
    with {:ok, context} <- request_context(opts, :read),
         do: command(client, "FERRICSTORE.TELEMETRY", ["CLUSTER_INFO"], context)
  end

  @doc "Reads safe namespace usage metadata."
  @spec namespace_usage(client(), binary(), keyword()) :: response()
  def namespace_usage(client, prefix, opts \\ []) do
    with {:ok, context} <- request_context(opts, :read),
         {:ok, prefix} <- ManagementInput.nonempty_binary(prefix, :namespace_usage, :prefix),
         do: command(client, "FERRICSTORE.TELEMETRY", ["NAMESPACE_USAGE", prefix], context)
  end

  @doc "Queries safe Flow observability metadata without payload search."
  @spec flow_query(client(), term(), keyword()) :: response()
  def flow_query(client, attrs \\ %{}, opts \\ []) do
    with {:ok, context} <- request_context(opts, :read),
         {:ok, args} <-
           ManagementInput.pair_args(
             attrs,
             :flow_query,
             :attrs,
             RequestContext.budget(context)
           ) do
      command(client, "FERRICSTORE.TELEMETRY", ["FLOW_QUERY" | args], context)
    end
  end

  @doc "Reads safe Flow history metadata without payload search."
  @spec flow_history(client(), binary(), term(), keyword()) :: response()
  def flow_history(client, id, attrs \\ %{}, opts \\ []) do
    with {:ok, context} <- request_context(opts, :read),
         {:ok, id} <- ManagementInput.nonempty_binary(id, :flow_history, :id),
         {:ok, args} <-
           ManagementInput.pair_args(
             attrs,
             :flow_history,
             :attrs,
             RequestContext.budget(context)
           ) do
      command(client, "FERRICSTORE.TELEMETRY", ["FLOW_HISTORY", id | args], context)
    end
  end

  defp command(client, command, args, %RequestContext{} = context),
    do: PreparedRequests.command_exec(client, command, args, context)

  defp request_context(opts, :read) do
    with {:ok, context} <- PreparedRequests.prepare_command_context(opts) do
      context =
        case RequestContext.option(context, :idempotent, :missing) do
          :missing -> RequestContext.put_option(context, :idempotent, true)
          _explicit -> context
        end

      {:ok, context}
    end
  end

  defp request_context(opts, :write), do: PreparedRequests.prepare_command_context(opts)
end
