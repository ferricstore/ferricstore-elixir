defmodule FerricStore.Protocol.MapKey do
  @moduledoc false

  @spec normalize!(term()) :: binary()
  def normalize!(value) when is_binary(value), do: value
  def normalize!(value) when is_atom(value), do: Atom.to_string(value)

  def normalize!(_value), do: invalid_key!()

  defp invalid_key!, do: raise(ArgumentError, "cannot encode native map key")
end
