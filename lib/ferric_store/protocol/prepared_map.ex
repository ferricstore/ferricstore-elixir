defmodule FerricStore.Protocol.PreparedMap do
  @moduledoc false

  alias FerricStore.Protocol.{MapKey, ValueCodec}
  alias FerricStore.RequestLimits

  @map_header_bytes 5
  @max_entries RequestLimits.max_batch_items()

  @enforce_keys [:entries, :entry_count, :byte_size, :keys, :reserved]
  defstruct [:entries, :entry_count, :byte_size, :keys, :metadata, :reserved]

  @type t :: %__MODULE__{
          entries: iodata(),
          entry_count: non_neg_integer(),
          byte_size: non_neg_integer(),
          keys: MapSet.t(binary()),
          metadata: term(),
          reserved: %{optional(binary()) => pos_integer()}
        }

  @spec prepare(map(), pos_integer(), [{binary() | atom(), term()}]) ::
          {:ok, t()} | {:error, :too_large}
  def prepare(payload, max_bytes, reserved_entries \\ [])
      when is_map(payload) and is_integer(max_bytes) and max_bytes > 0 and
             is_list(reserved_entries) do
    entry_count = map_size(payload)
    validate_entry_count!(entry_count + length(reserved_entries))

    prepare_encoded(max_bytes, reserved_entries, fn remaining ->
      case encode_entries(payload, remaining) do
        {:ok, entries, keys, remaining} ->
          {:ok, entries, entry_count, keys, remaining}

        :too_large ->
          :too_large
      end
    end)
  end

  @doc false
  @spec prepare_encoded(pos_integer(), [{binary() | atom(), term()}], function()) ::
          {:ok, t()} | {:error, term()}
  def prepare_encoded(max_bytes, reserved_entries, encoder)
      when is_integer(max_bytes) and max_bytes > 0 and is_list(reserved_entries) and
             is_function(encoder, 1) do
    with {:ok, remaining} <- reserve(max_bytes, @map_header_bytes),
         {:ok, entries, entry_count, keys, remaining} <- encoder.(remaining),
         :ok <- validate_entry_count!(entry_count + length(reserved_entries)),
         base_size = max_bytes - remaining,
         {:ok, reserved, keys, _remaining} <-
           reserve_entries(reserved_entries, keys, remaining, %{}) do
      {:ok,
       %__MODULE__{
         entries: entries,
         entry_count: entry_count,
         byte_size: base_size,
         keys: keys,
         reserved: reserved
       }}
    else
      :too_large -> {:error, :too_large}
      {:error, _reason} = error -> error
    end
  end

  @spec put_metadata(t(), term()) :: t()
  def put_metadata(%__MODULE__{} = prepared, metadata),
    do: %{prepared | metadata: metadata}

  @spec metadata(t()) :: term()
  def metadata(%__MODULE__{metadata: metadata}), do: metadata

  @spec put_reserved(t(), binary() | atom(), term()) :: t()
  def put_reserved(%__MODULE__{} = prepared, key, value) do
    normalized_key = MapKey.normalize!(key)

    case Map.pop(prepared.reserved, normalized_key) do
      {nil, _reserved} ->
        if MapSet.member?(prepared.keys, normalized_key) do
          prepared
        else
          raise ArgumentError,
                "prepared map has no reservation for key #{inspect(normalized_key)}"
        end

      {reserved_bytes, reserved} ->
        case encode_entry(normalized_key, value, reserved_bytes) do
          {:ok, entry, entry_bytes, _remaining} ->
            %{
              prepared
              | entries: [prepared.entries, entry],
                entry_count: prepared.entry_count + 1,
                byte_size: prepared.byte_size + entry_bytes,
                keys: MapSet.put(prepared.keys, normalized_key),
                reserved: reserved
            }

          :too_large ->
            raise ArgumentError,
                  "prepared value for #{inspect(normalized_key)} exceeds its wire reservation"
        end
    end
  end

  @spec to_iodata(t()) :: iodata()
  def to_iodata(%__MODULE__{reserved: reserved} = prepared) when map_size(reserved) == 0,
    do: [<<6, prepared.entry_count::32>>, prepared.entries]

  def to_iodata(%__MODULE__{reserved: reserved}) do
    raise ArgumentError,
          "prepared map still has unresolved entries: #{inspect(Map.keys(reserved))}"
  end

  defp encode_entries(payload, remaining) do
    payload
    |> Enum.reduce_while({[], MapSet.new(), remaining}, fn {key, value},
                                                           {entries, keys, remaining} ->
      normalized_key = MapKey.normalize!(key)
      ensure_unique_key!(keys, normalized_key)

      case encode_entry(normalized_key, value, remaining) do
        {:ok, entry, _entry_bytes, remaining} ->
          {:cont, {[entry | entries], MapSet.put(keys, normalized_key), remaining}}

        :too_large ->
          {:halt, :too_large}
      end
    end)
    |> case do
      {entries, keys, remaining} -> {:ok, Enum.reverse(entries), keys, remaining}
      :too_large -> :too_large
    end
  end

  defp reserve_entries([], keys, remaining, reserved),
    do: {:ok, reserved, keys, remaining}

  defp reserve_entries([{key, value} | entries], keys, remaining, reserved) do
    normalized_key = MapKey.normalize!(key)
    ensure_unique_key!(keys, normalized_key)

    case encode_entry(normalized_key, value, remaining) do
      {:ok, _entry, entry_bytes, remaining} ->
        reserve_entries(
          entries,
          MapSet.put(keys, normalized_key),
          remaining,
          Map.put(reserved, normalized_key, entry_bytes)
        )

      :too_large ->
        :too_large
    end
  end

  defp encode_entry(key, value, remaining) do
    key_bytes = byte_size(key) + 4

    with {:ok, value_budget} <- reserve(remaining, key_bytes),
         {:ok, encoded, value_bytes} <-
           ValueCodec.encode_iodata_at_depth(value, 1, value_budget) do
      entry_bytes = key_bytes + value_bytes
      {:ok, [<<byte_size(key)::32>>, key, encoded], entry_bytes, remaining - entry_bytes}
    else
      :too_large -> :too_large
      {:error, :too_large} -> :too_large
    end
  end

  defp reserve(remaining, bytes) when bytes <= remaining, do: {:ok, remaining - bytes}
  defp reserve(_remaining, _bytes), do: :too_large

  defp ensure_unique_key!(keys, key) do
    if MapSet.member?(keys, key) do
      raise ArgumentError, "duplicate normalized map key #{inspect(key)}"
    end
  end

  defp validate_entry_count!(count) when count <= @max_entries, do: :ok

  defp validate_entry_count!(_count) do
    raise ArgumentError, "native protocol map exceeds #{@max_entries} entries"
  end
end
