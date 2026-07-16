defmodule FerricStore.Flow.CodecRuntime do
  @moduledoc false

  alias FerricStore.Codec.Raw
  alias FerricStore.{DeadlineBudget, DeadlineTask}
  alias FerricStore.Flow.CodecError

  @spec encode(module(), term()) :: binary()
  def encode(codec, value) when is_atom(codec) do
    case invoke_encode(codec, value) do
      encoded when is_binary(encoded) -> encoded
      _invalid -> fail(codec)
    end
  end

  @spec encode(module(), term(), DeadlineBudget.t()) ::
          {:ok, binary()} | {:error, :timeout}
  def encode(Raw, value, %DeadlineBudget{} = budget) when is_binary(value) do
    case DeadlineBudget.ensure_active(budget) do
      :ok -> {:ok, value}
      {:error, :timeout} = error -> error
    end
  end

  def encode(codec, value, %DeadlineBudget{} = budget) when is_atom(codec) do
    run(budget, codec, fn -> encode(codec, value) end)
  end

  @spec run(DeadlineBudget.t(), module(), (-> result)) :: {:ok, result} | {:error, :timeout}
        when result: term()
  def run(%DeadlineBudget{} = budget, codec, function)
      when is_atom(codec) and is_function(function, 0) do
    budget
    |> DeadlineTask.run(function)
    |> restore_failure(codec)
  end

  defp invoke_encode(codec, value) do
    codec.encode(value)
  rescue
    _error -> fail(codec)
  catch
    _kind, _reason -> fail(codec)
  end

  defp restore_failure({:error, {:deadline_task_failed, {:error, error}}}, _codec),
    do: raise(error)

  defp restore_failure({:error, {:deadline_task_failed, _reason}}, codec), do: fail(codec)
  defp restore_failure(result, _codec), do: result

  defp fail(codec), do: raise(CodecError, codec: codec, operation: :encode)
end
