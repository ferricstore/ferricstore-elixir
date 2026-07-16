defmodule FerricStore.Flow.ResponseDecodeRuntime do
  @moduledoc false

  alias FerricStore.{DeadlineTask, RequestContext, Result}

  def run(%RequestContext{} = context, codec, decoder) do
    case DeadlineTask.run(RequestContext.budget(context), fn -> safely_decode(codec, decoder) end) do
      {:ok, result} -> result
      {:error, :timeout} -> Result.error(:timeout)
      {:error, {:deadline_task_failed, _reason}} -> codec_error(codec)
    end
  end

  defp safely_decode(codec, decoder) do
    decoder.()
  rescue
    _error -> codec_error(codec)
  catch
    _kind, _reason -> codec_error(codec)
  end

  defp codec_error(codec), do: Result.error({:flow_codec_decode_failed, codec})
end
