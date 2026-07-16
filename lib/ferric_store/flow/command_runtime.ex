defmodule FerricStore.Flow.CommandRuntime do
  @moduledoc false

  alias FerricStore.Flow.{CodecError, Options}
  alias FerricStore.{RequestContext, Result}

  @spec with_options(atom(), term(), (keyword(), RequestContext.t() -> term())) :: term()
  def with_options(operation, opts, function) when is_function(function, 2) do
    case Options.prepare_request(operation, opts) do
      {:ok, prepared, context} ->
        result = function.(prepared, context)

        case RequestContext.ensure_active(context) do
          :ok -> result
          {:error, :timeout} -> Result.error(:timeout)
        end

      {:error, reason} ->
        Result.error(reason)
    end
  rescue
    error in CodecError -> Result.error({:flow_codec_encode_failed, error.codec})
  end

  @spec empty_batch(atom(), term()) :: [] | {:error, FerricStore.Error.t()}
  def empty_batch(operation, opts) do
    case Options.validate_noop_request(operation, opts) do
      :ok -> []
      {:error, reason} -> Result.error(reason)
    end
  end
end
