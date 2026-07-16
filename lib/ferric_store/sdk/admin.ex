defmodule FerricStore.SDK.Admin do
  @moduledoc """
  Native FerricStore cluster and observability commands.
  """

  alias FerricStore.Protocol.Opcodes
  alias FerricStore.RequestContext
  alias FerricStore.RouteKey
  alias FerricStore.SDK.Native.PreparedRequests
  alias FerricStore.Types

  @admin_commands [
    cluster_health: :cluster_health,
    cluster_stats: :cluster_stats,
    cluster_keyslot: :cluster_keyslot,
    cluster_slots: :cluster_slots,
    cluster_status: :cluster_status,
    cluster_join: :cluster_join,
    cluster_leave: :cluster_leave,
    cluster_failover: :cluster_failover,
    cluster_promote: :cluster_promote,
    cluster_demote: :cluster_demote,
    cluster_role: :cluster_role,
    ferricstore_key_info: :ferricstore_key_info,
    ferricstore_config: :ferricstore_config,
    ferricstore_hotness: :ferricstore_hotness,
    ferricstore_metrics: :ferricstore_metrics,
    ferricstore_blobgc: :ferricstore_blobgc
  ]
  @opcodes Map.new(@admin_commands, fn {function, opcode_name} ->
             {function, Opcodes.fetch!(opcode_name)}
           end)

  for {function, opcode_name} <- @admin_commands do
    def unquote(function)(client, payload \\ %{}, opts \\ []) do
      request(client, unquote(opcode_name), payload, opts)
    end
  end

  @spec request(pid(), non_neg_integer() | atom() | binary(), term(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def request(client, opcode, payload \\ %{}, opts \\ []) do
    with :ok <- validate_payload(payload),
         {:ok, context} <- PreparedRequests.prepare(opts, [:route_key]),
         {:ok, payload} <- normalize_payload(payload, context) do
      case RouteKey.resolve(payload, opts, [:route_key], ["key"]) do
        {:ok, key} -> PreparedRequests.request_by_key(client, opcode, key, payload, context)
        :none -> PreparedRequests.request(client, opcode, payload, context)
        {:error, reason} -> {:error, reason}
      end
    end
  end

  @spec opcodes() :: map()
  def opcodes, do: @opcodes

  defp validate_payload(payload) when is_map(payload), do: :ok

  defp validate_payload(payload),
    do: {:error, {:invalid_admin_payload, %{reason: :expected_map, value: payload}}}

  defp normalize_payload(payload, context),
    do: Types.normalize_map_result(payload, RequestContext.budget(context))
end
