defmodule FerricStore.Flow.Response do
  @moduledoc false

  alias FerricStore.Codec.Raw
  alias FerricStore.{RequestContext, Result}

  alias FerricStore.Flow.{
    ClaimResponseDecoder,
    RecordResponseDecoder,
    ResponseDecodeRuntime,
    ResponseRecords,
    ValueResponse
  }

  @spec decode_record(term(), keyword(), RequestContext.t(), atom()) :: term()
  def decode_record({:error, _reason} = error, _opts, _context, _operation), do: error

  def decode_record(value, opts, %RequestContext{} = context, operation) do
    codec = Keyword.get(opts, :codec, Raw)

    if codec == Raw do
      RecordResponseDecoder.decode_record_raw(value, operation, RequestContext.budget(context))
    else
      ResponseDecodeRuntime.run(context, codec, fn ->
        RecordResponseDecoder.decode_record(value, operation, codec)
      end)
    end
  end

  @spec decode_list(term(), keyword(), RequestContext.t(), atom()) :: term()
  def decode_list({:error, _reason} = error, _opts, _context, _operation), do: error

  def decode_list(values, opts, %RequestContext{} = context, operation) do
    codec = Keyword.get(opts, :codec, Raw)

    if codec == Raw do
      RecordResponseDecoder.decode_list_raw(values, operation, RequestContext.budget(context))
    else
      ResponseDecodeRuntime.run(context, codec, fn ->
        RecordResponseDecoder.decode_list(values, operation, codec)
      end)
    end
  end

  @spec decode_claims(term(), keyword(), RequestContext.t()) :: term()
  def decode_claims({:error, _reason} = error, _opts, _context), do: error

  def decode_claims(jobs, opts, %RequestContext{} = context) when is_list(jobs) do
    codec = Keyword.get(opts, :codec, Raw)

    if codec == Raw do
      ClaimResponseDecoder.decode_raw(jobs, RequestContext.budget(context))
    else
      ResponseDecodeRuntime.run(context, codec, fn -> ClaimResponseDecoder.decode(jobs, codec) end)
    end
  end

  def decode_claims(_other, _opts, context) do
    case RequestContext.ensure_active(context) do
      :ok -> ClaimResponseDecoder.invalid(:expected_list)
      {:error, :timeout} -> Result.error(:timeout)
    end
  end

  @spec decode_values(term(), keyword(), list(), RequestContext.t()) :: term()
  def decode_values(values, opts, refs, %RequestContext{} = context) do
    codec = Keyword.get(opts, :codec, Raw)

    if codec == Raw do
      ValueResponse.decode_raw(values, refs, RequestContext.budget(context))
    else
      ResponseDecodeRuntime.run(context, codec, fn ->
        ValueResponse.decode(values, refs, codec)
      end)
    end
  end

  @spec decode_record_list_or_response(term(), keyword(), RequestContext.t()) :: term()
  def decode_record_list_or_response({:error, _reason} = error, _opts, _context), do: error

  def decode_record_list_or_response(value, opts, %RequestContext{} = context) do
    codec = Keyword.get(opts, :codec, Raw)

    if codec == Raw do
      value
    else
      ResponseDecodeRuntime.run(context, codec, fn -> ResponseRecords.decode(value, codec) end)
    end
  end
end
