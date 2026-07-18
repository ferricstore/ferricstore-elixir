defmodule FerricStore.SDK.Native.Topology do
  @moduledoc """
  Read-only snapshot of the cluster topology used by the topology-aware SDK.

  `FerricStore.SDK.topology/1` returns this struct. `route_epoch` identifies the
  server routing version, `shard_count` is the number of represented shards,
  `control_endpoint` caches the deterministic control-plane target, and
  `endpoints` contains effective endpoints keyed by transport identity.
  `slots` is the SDK's immutable 1,024-entry routing table.

  Applications may inspect a snapshot, but routing should go through
  `FerricStore.SDK.route/2` so future topology updates are observed.
  """

  alias FerricStore.RouteKey
  alias FerricStore.RoutingSlot
  alias FerricStore.SDK.Native.{EndpointIdentity, Topology.Builder}

  @num_slots 1024
  @max_route_key_bytes RouteKey.max_bytes()
  defstruct route_epoch: 0,
            shard_count: 0,
            slots: List.to_tuple(List.duplicate(nil, @num_slots)),
            endpoints: %{},
            control_endpoint: nil

  @type t :: %__MODULE__{
          route_epoch: non_neg_integer(),
          shard_count: non_neg_integer(),
          slots: tuple(),
          endpoints: %{optional(connection_key()) => endpoint()},
          control_endpoint: endpoint() | nil
        }

  @type endpoint :: %{
          required(:node) => binary(),
          required(:host) => binary(),
          required(:native_port) => non_neg_integer(),
          optional(:native_tls_port) => non_neg_integer(),
          optional(:tls) => boolean(),
          optional(:server_name) => binary() | charlist(),
          optional(:verify) => boolean(),
          optional(:tls_verify) => boolean(),
          optional(:cacertfile) => binary() | charlist(),
          optional(:cacerts) => list() | struct()
        }

  @type connection_key ::
          {:gen_tcp, binary(), non_neg_integer(), term()}
          | {:ssl, binary(), non_neg_integer(), term(), term()}

  @spec build(map(), keyword()) :: {:ok, t()} | {:error, term()}
  def build(payload, opts \\ []) do
    case Builder.build(payload, opts) do
      {:ok, fields} -> {:ok, struct!(__MODULE__, fields)}
      {:error, _reason} = error -> error
    end
  end

  @doc false
  @spec prepare_endpoint(map()) :: map()
  def prepare_endpoint(endpoint) when is_map(endpoint), do: EndpointIdentity.prepare(endpoint)

  @spec route_key(t(), term()) :: {:ok, map()} | {:error, term()}
  def route_key(%__MODULE__{} = topology, key)
      when is_binary(key) and byte_size(key) <= @max_route_key_bytes do
    slot = RoutingSlot.for_key(key)

    case elem(topology.slots, slot) do
      nil -> {:error, {:unmapped_slot, slot}}
      route -> {:ok, Map.put(route, :slot, slot)}
    end
  end

  def route_key(%__MODULE__{} = topology, {:slot, slot})
      when is_integer(slot) and slot >= 0 and slot < @num_slots do
    case elem(topology.slots, slot) do
      nil -> {:error, {:unmapped_slot, slot}}
      route -> {:ok, Map.put(route, :slot, slot)}
    end
  end

  def route_key(%__MODULE__{}, key), do: RouteKey.validate(key)

  @spec slot_for_key(binary()) :: non_neg_integer()
  def slot_for_key(key), do: RoutingSlot.for_key(key)

  @spec endpoint_key(map()) :: connection_key()
  def endpoint_key(endpoint) when is_map(endpoint), do: EndpointIdentity.key(endpoint)
end
