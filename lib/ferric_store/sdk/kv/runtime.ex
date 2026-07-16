defmodule FerricStore.SDK.KV.Runtime do
  @moduledoc false

  alias FerricStore.RequestContext
  alias FerricStore.SDK.KV.Options

  @spec call(atom(), keyword(), (RequestContext.t() -> term())) :: term()
  def call(operation, options, command) when is_atom(operation) and is_function(command, 1) do
    with {:ok, context} <- Options.validate(operation, options) do
      result = command.(context)

      case RequestContext.ensure_active(context) do
        :ok -> result
        {:error, :timeout} = timeout -> timeout
      end
    end
  end
end
