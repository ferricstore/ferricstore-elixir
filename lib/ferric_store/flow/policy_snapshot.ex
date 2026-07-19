defmodule FerricStore.Flow.PolicySnapshot do
  @moduledoc """
  Typed snapshot returned by Flow policy reads and mutations.

  `generation` is allocated monotonically by FerricStore. Pass a snapshot's
  generation as `expected_generation` to perform a compare-and-swap update.
  """

  alias FerricStore.Flow.PolicyUpdateValidator
  alias FerricStore.Types

  @max_generation PolicyUpdateValidator.max_generation()

  @known_fields ~w(
    type generation state version mode max_active_ms retry retention
    indexed_attributes indexed_state_meta governance states
  )
  @known_atom_fields ~w(
    type generation state version mode max_active_ms retry retention
    indexed_attributes indexed_state_meta governance states
  )a

  @enforce_keys [:type, :generation]
  defstruct [
    :type,
    :generation,
    :state,
    :version,
    :mode,
    :max_active_ms,
    :retry,
    :retention,
    :indexed_attributes,
    :indexed_state_meta,
    :governance,
    states: %{},
    extensions: %{}
  ]

  @type generation :: non_neg_integer()
  @type state_mode :: :fifo | :parallel | nil

  @type t :: %__MODULE__{
          type: binary(),
          generation: generation(),
          state: binary() | nil,
          version: term(),
          mode: state_mode(),
          max_active_ms: pos_integer() | :infinity | nil,
          retry: map() | nil,
          retention: map() | nil,
          indexed_attributes: [binary()] | nil,
          indexed_state_meta: binary() | [binary()] | nil,
          governance: map() | nil,
          states: %{optional(binary()) => map()},
          extensions: map()
        }

  @doc false
  @spec decode(term(), binary()) :: {:ok, t()} | {:error, term()}
  def decode(value, expected_type) when is_map(value) and is_binary(expected_type) do
    with {:ok, type} <- type(value, expected_type),
         {:ok, generation} <- generation(value),
         {:ok, state} <- optional_state(value),
         {:ok, mode} <- mode(value),
         {:ok, states} <- states(value) do
      {:ok,
       %__MODULE__{
         type: type,
         generation: generation,
         state: state,
         version: Types.get(value, "version"),
         mode: mode,
         max_active_ms: normalize_infinity(Types.get(value, "max_active_ms")),
         retry: Types.get(value, "retry"),
         retention: Types.get(value, "retention"),
         indexed_attributes: Types.get(value, "indexed_attributes"),
         indexed_state_meta: Types.get(value, "indexed_state_meta"),
         governance: Types.get(value, "governance"),
         states: states,
         extensions: Map.drop(value, @known_fields ++ @known_atom_fields)
       }}
    end
  end

  def decode(value, _expected_type), do: invalid(:snapshot, value)

  defp type(value, expected_type) do
    case Types.get(value, "type") do
      ^expected_type -> {:ok, expected_type}
      actual -> invalid(:type, actual)
    end
  end

  defp generation(value) do
    case Types.get(value, "generation") do
      generation
      when is_integer(generation) and generation >= 0 and
             generation <= @max_generation ->
        {:ok, generation}

      actual ->
        invalid(:generation, actual)
    end
  end

  defp optional_state(value) do
    case Types.get(value, "state") do
      nil -> {:ok, nil}
      binary when is_binary(binary) -> {:ok, binary}
      actual -> invalid(:state, actual)
    end
  end

  defp mode(value) do
    case Types.get(value, "mode") do
      nil -> {:ok, nil}
      :fifo -> {:ok, :fifo}
      :parallel -> {:ok, :parallel}
      "fifo" -> {:ok, :fifo}
      "parallel" -> {:ok, :parallel}
      actual -> invalid(:mode, actual)
    end
  end

  defp states(value) do
    case Types.get(value, "states", %{}) do
      states when is_map(states) -> {:ok, states}
      actual -> invalid(:states, actual)
    end
  end

  defp normalize_infinity("infinity"), do: :infinity
  defp normalize_infinity(value), do: value

  defp invalid(field, value),
    do: {:error, {:invalid_policy_snapshot, %{field: field, value: value}}}
end
