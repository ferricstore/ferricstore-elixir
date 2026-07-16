defmodule FerricStore.SDK.Invocation do
  @moduledoc """
  FerricStore Enterprise invocation helpers.

  These helpers keep Enterprise callers on the same stable native SDK surface as
  the rest of FerricStore. They build the public `INVOCATION.*` commands and
  send them through `FerricStore.SDK.Native.Client.command_exec/4`, so normal SDK
  routing, timeouts, authentication, and trusted request-context handling still
  apply.
  """

  alias FerricStore.{DeadlineBudget, RequestContext}

  alias FerricStore.SDK.{
    InvocationInput,
    InvocationOptions,
    Native.Client,
    Native.PreparedRequests
  }

  @envelope_option_keys [:context, :idempotency_key, :scope]

  @type client :: pid()
  @type response :: {:ok, term()} | {:error, term()}

  @doc """
  Create or replace an invocation definition.

  Accepts either a JSON string or a map. Map input is encoded as JSON.
  """
  @spec put_definition(client(), map() | binary(), keyword()) :: response()
  def put_definition(client, definition, opts \\ []) do
    with :ok <- InvocationOptions.validate(opts),
         {:ok, context} <- request_context(opts),
         {:ok, definition} <-
           InvocationInput.definition(definition, RequestContext.budget(context)) do
      command_with_context(client, "INVOCATION.DEFINITION.PUT", [definition], context)
    end
  end

  @doc """
  Fetch one invocation definition by name.
  """
  @spec get_definition(client(), binary(), keyword()) :: response()
  def get_definition(client, name, opts \\ []) do
    with :ok <- InvocationOptions.validate(opts),
         {:ok, name} <- InvocationInput.nonempty_binary(name, :get_definition, :name) do
      read_command(client, "INVOCATION.DEFINITION.GET", [name], opts)
    end
  end

  @doc """
  List invocation definitions.
  """
  @spec list_definitions(client(), keyword()) :: response()
  def list_definitions(client, opts \\ []) do
    with :ok <- InvocationOptions.validate(opts) do
      read_command(client, "INVOCATION.DEFINITION.LIST", [], opts)
    end
  end

  @doc """
  Create an invocation from a named definition.

  `attrs` are persisted invocation attributes. Optional create envelope fields:

  - `:context` - caller-supplied context embedded in the invocation envelope.
  - `:idempotency_key` - optional idempotency key understood by Enterprise.
  - `:request_context` - trusted proxy context sent out-of-band to the server.
  """
  @spec create(client(), binary(), map(), keyword()) :: response()
  def create(client, name, attrs, opts \\ []) do
    with :ok <- InvocationOptions.validate(opts, [:context, :idempotency_key]),
         {:ok, context} <- request_context(opts) do
      case build_create_args(name, attrs, opts, RequestContext.budget(context)) do
        args when is_list(args) ->
          command_with_context(client, "INVOCATION.CREATE", args, context)

        {:error, _reason} = error ->
          error
      end
    end
  end

  @doc """
  Fetch one invocation by id.
  """
  @spec get(client(), binary(), keyword()) :: response()
  def get(client, id, opts \\ []) do
    with :ok <- InvocationOptions.validate(opts),
         {:ok, id} <- InvocationInput.nonempty_binary(id, :get, :id) do
      read_command(client, "INVOCATION.GET", [id], opts)
    end
  end

  @doc """
  List known invocation partitions for a definition.

  Pass `scope: "..."` to restrict the partition list.
  """
  @spec list_partitions(client(), binary(), keyword()) :: response()
  def list_partitions(client, name, opts \\ []) do
    case partition_list_args(name, opts) do
      args when is_list(args) -> read_command(client, "INVOCATION.PARTITION.LIST", args, opts)
      {:error, _reason} = error -> error
    end
  end

  @doc false
  @spec definition_put_args(term()) :: [binary()] | {:error, term()}
  def definition_put_args(definition) do
    case InvocationInput.definition(definition, DeadlineBudget.new(:infinity)) do
      {:ok, definition} -> [definition]
      {:error, _reason} = error -> error
    end
  end

  @doc false
  @spec create_args(term(), term(), term()) :: [binary()] | {:error, term()}
  def create_args(name, attrs, opts \\ []) do
    with :ok <- InvocationOptions.validate(opts, [:context, :idempotency_key]) do
      build_create_args(name, attrs, opts, DeadlineBudget.new(:infinity))
    end
  end

  defp build_create_args(name, attrs, opts, budget) do
    with {:ok, name} <- InvocationInput.nonempty_binary(name, :create, :name),
         {:ok, attrs} <- InvocationInput.map(attrs, :create, :attrs),
         :ok <- InvocationOptions.optional_binary(opts, :idempotency_key, :create),
         envelope =
           %{"attrs" => attrs}
           |> put_if_present("context", Keyword.get(opts, :context))
           |> put_if_present("idempotency_key", Keyword.get(opts, :idempotency_key)),
         {:ok, json} <- InvocationInput.json(envelope, :create, :payload, budget) do
      [name, json]
    end
  end

  @doc false
  @spec partition_list_args(term(), term()) :: [binary()] | {:error, term()}
  def partition_list_args(name, opts \\ []) do
    with :ok <- InvocationOptions.validate(opts, [:scope]),
         {:ok, name} <- InvocationInput.nonempty_binary(name, :list_partitions, :name),
         {:ok, scope} <- InvocationOptions.scope(opts) do
      case scope do
        nil -> [name]
        scope -> [name, "SCOPE", scope]
      end
    end
  end

  defp command(client, command, args, opts) do
    Client.command_exec(client, command, args, Keyword.drop(opts, @envelope_option_keys))
  end

  defp command_with_context(client, command, args, %RequestContext{} = context),
    do: PreparedRequests.command_exec(client, command, args, context)

  defp read_command(client, command, args, opts),
    do: command(client, command, args, Keyword.put_new(opts, :idempotent, true))

  defp put_if_present(map, _key, nil), do: map
  defp put_if_present(map, key, value), do: Map.put(map, key, value)

  defp request_context(opts) do
    opts
    |> Keyword.drop(@envelope_option_keys)
    |> PreparedRequests.prepare_command_context()
  end
end
