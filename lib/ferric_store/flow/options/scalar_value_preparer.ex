defmodule FerricStore.Flow.Options.ScalarValuePreparer do
  @moduledoc false

  alias FerricStore.Codec.Raw
  alias FerricStore.DeadlineBudget
  alias FerricStore.Flow.CodecRuntime
  alias FerricStore.Flow.Options.PreparedValue

  @encoded_options %{
    create: [:payload],
    transition: [:payload],
    complete: [:result, :payload],
    complete_many: [:result, :payload],
    retry: [:error, :payload],
    fail: [:error, :payload],
    cancel: [:reason]
  }

  @spec prepare(atom(), keyword(), DeadlineBudget.t()) ::
          {:ok, keyword()} | {:error, :timeout}
  def prepare(operation, opts, %DeadlineBudget{} = budget) do
    fields = Map.get(@encoded_options, operation, [])
    codec = Keyword.get(opts, :codec, Raw)

    if encoding_required?(opts, fields, codec) do
      CodecRuntime.run(budget, codec, fn -> encode_fields(opts, fields, codec) end)
    else
      case DeadlineBudget.ensure_active(budget) do
        :ok -> {:ok, opts}
        {:error, :timeout} = error -> error
      end
    end
  end

  defp encoding_required?(opts, fields, codec) do
    Enum.any?(fields, fn field ->
      case Keyword.fetch(opts, field) do
        {:ok, value} when not is_nil(value) -> codec != Raw or not is_binary(value)
        _missing_or_nil -> false
      end
    end)
  end

  defp encode_fields(opts, fields, codec) do
    Enum.reduce(fields, opts, fn field, prepared ->
      case Keyword.fetch(prepared, field) do
        {:ok, value} when not is_nil(value) ->
          Keyword.replace!(prepared, field, PreparedValue.new(CodecRuntime.encode(codec, value)))

        _missing_or_nil ->
          prepared
      end
    end)
  end
end
