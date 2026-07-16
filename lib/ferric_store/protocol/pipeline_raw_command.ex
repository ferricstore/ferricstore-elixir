defmodule FerricStore.Protocol.PipelineRawCommand do
  @moduledoc false

  alias FerricStore.{BoundedList, RequestLimits}
  alias FerricStore.Protocol.CommandName

  @max_arguments RequestLimits.max_command_items()

  @spec fields(term(), term(), non_neg_integer()) ::
          {:ok, {:raw, binary(), list()}} | {:error, term()}
  def fields(name, [], index) when is_binary(name) do
    case CommandName.normalize(name) do
      {:ok, name} -> {:ok, {:raw, name, []}}
      {:error, _reason} -> error(index, :invalid_command_name)
    end
  end

  def fields(name, args, index) when is_binary(name) do
    with {:ok, name} <- normalize_name(name, index),
         :ok <- validate_arguments(args, index) do
      {:ok, {:raw, name, args}}
    end
  end

  def fields(_name, _args, index), do: error(index, :invalid_command_name)

  defp normalize_name(name, index) do
    case CommandName.normalize(name) do
      {:ok, name} -> {:ok, name}
      {:error, _reason} -> error(index, :invalid_command_name)
    end
  end

  defp validate_arguments(args, index) do
    case BoundedList.count(args, @max_arguments) do
      {:ok, _count} -> :ok
      {:error, :improper_list} -> error(index, :invalid_command_arguments)
      {:error, {:limit_exceeded, _observed}} -> error(index, :too_many_command_arguments)
    end
  end

  defp error(index, reason),
    do: {:error, {:invalid_pipeline_command, %{index: index, reason: reason}}}
end
