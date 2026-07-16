defmodule FerricStore.Flow.HistoryResponse do
  @moduledoc false

  alias FerricStore.Codec.Raw
  alias FerricStore.Flow.{HistoryResponseDecoder, ResponseDecodeRuntime}
  alias FerricStore.RequestContext

  @spec decode(term(), keyword(), RequestContext.t(), atom()) :: term()
  def decode({:error, _reason} = error, _opts, _context, _operation), do: error

  def decode(values, opts, %RequestContext{} = context, operation) do
    codec = Keyword.get(opts, :codec, Raw)

    if codec == Raw do
      HistoryResponseDecoder.decode_raw(values, operation, RequestContext.budget(context))
    else
      ResponseDecodeRuntime.run(context, codec, fn ->
        HistoryResponseDecoder.decode(values, operation, codec)
      end)
    end
  end
end
