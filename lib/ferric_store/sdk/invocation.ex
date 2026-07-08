defmodule FerricStore.SDK.Invocation do
  @moduledoc """
  FerricStore Enterprise invocation helpers.

  These helpers keep Enterprise callers on the same stable native SDK surface as
  the rest of FerricStore. They build the public `INVOCATION.*` commands and
  send them through `FerricStore.SDK.Native.Client.command_exec/4`, so normal SDK
  routing, timeouts, authentication, and trusted request-context handling still
  apply.
  """

  alias FerricStore.SDK.Native.Client

  @type client :: GenServer.server()
  @type response :: {:ok, term()} | {:error, term()}

  @doc """
  Create or replace an invocation definition.

  Accepts either a JSON string or a map. Map input is encoded as JSON.
  """
  @spec put_definition(client(), map() | binary(), keyword()) :: response()
  def put_definition(client, definition, opts \\ []) do
    command(client, "INVOCATION.DEFINITION.PUT", definition_put_args(definition), opts)
  end

  @doc """
  Fetch one invocation definition by name.
  """
  @spec get_definition(client(), binary(), keyword()) :: response()
  def get_definition(client, name, opts \\ []) when is_binary(name) do
    command(client, "INVOCATION.DEFINITION.GET", [name], opts)
  end

  @doc """
  List invocation definitions.
  """
  @spec list_definitions(client(), keyword()) :: response()
  def list_definitions(client, opts \\ []) do
    command(client, "INVOCATION.DEFINITION.LIST", [], opts)
  end

  @doc """
  Create an invocation from a named definition.

  `attrs` are persisted invocation attributes. Optional create envelope fields:

  - `:context` - caller-supplied context embedded in the invocation envelope.
  - `:idempotency_key` - optional idempotency key understood by Enterprise.
  - `:request_context` - trusted proxy context sent out-of-band to the server.
  """
  @spec create(client(), binary(), map(), keyword()) :: response()
  def create(client, name, attrs, opts \\ []) when is_binary(name) and is_map(attrs) do
    command(client, "INVOCATION.CREATE", create_args(name, attrs, opts), opts)
  end

  @doc """
  Fetch one invocation by id.
  """
  @spec get(client(), binary(), keyword()) :: response()
  def get(client, id, opts \\ []) when is_binary(id) do
    command(client, "INVOCATION.GET", [id], opts)
  end

  @doc """
  List known invocation partitions for a definition.

  Pass `scope: "..."` to restrict the partition list.
  """
  @spec list_partitions(client(), binary(), keyword()) :: response()
  def list_partitions(client, name, opts \\ []) when is_binary(name) do
    command(client, "INVOCATION.PARTITION.LIST", partition_list_args(name, opts), opts)
  end

  @doc false
  @spec definition_put_args(map() | binary()) :: [binary()]
  def definition_put_args(definition), do: [json(definition)]

  @doc false
  @spec create_args(binary(), map(), keyword()) :: [binary()]
  def create_args(name, attrs, opts \\ []) when is_binary(name) and is_map(attrs) do
    envelope =
      %{"attrs" => attrs}
      |> put_if_present("context", Keyword.get(opts, :context))
      |> put_if_present("idempotency_key", Keyword.get(opts, :idempotency_key))

    [name, json(envelope)]
  end

  @doc false
  @spec partition_list_args(binary(), keyword()) :: [binary()]
  def partition_list_args(name, opts \\ []) when is_binary(name) do
    case Keyword.get(opts, :scope) do
      nil -> [name]
      scope when is_binary(scope) -> [name, "SCOPE", scope]
    end
  end

  defp command(client, command, args, opts), do: Client.command_exec(client, command, args, opts)

  defp json(value) when is_binary(value), do: value
  defp json(value), do: Jason.encode!(value)

  defp put_if_present(map, _key, nil), do: map
  defp put_if_present(map, key, value), do: Map.put(map, key, value)
end
