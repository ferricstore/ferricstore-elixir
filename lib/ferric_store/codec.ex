defmodule FerricStore.Codec do
  @moduledoc """
  Value codec behaviour used by Flow helpers.

  The native protocol always sends bytes to FerricStore. Codecs decide how Elixir
  values become those bytes for Flow payloads, results, errors, and value refs.
  """

  @callback encode(term()) :: binary()
  @callback decode(binary()) :: term()
end

defmodule FerricStore.Codec.Raw do
  @moduledoc """
  Raw binary codec. Non-binary values are converted with `to_string/1`.
  """

  @behaviour FerricStore.Codec

  @impl true
  def encode(value) when is_binary(value), do: value
  def encode(value), do: to_string(value)

  @impl true
  def decode(value), do: value
end

defmodule FerricStore.Codec.Term do
  @moduledoc """
  Erlang external term codec for Elixir-only applications.

  Use this when producers and workers are Elixir services. For cross-language
  workflows, prefer an explicit JSON/MessagePack codec in your application.
  """

  @behaviour FerricStore.Codec

  @impl true
  def encode(value), do: :erlang.term_to_binary(value)

  @impl true
  def decode(value) when is_binary(value), do: :erlang.binary_to_term(value)
end
