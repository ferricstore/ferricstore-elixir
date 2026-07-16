defmodule FerricStore.SDK.InvocationJSON do
  @moduledoc false

  alias FerricStore.{DeadlineBudget, DeadlineTask}
  alias FerricStore.SDK.{InvocationJSONInputValidator, InvocationJSONValidator}
  alias Jason.OrderedObject

  @spec validate_object(binary(), DeadlineBudget.t()) ::
          :ok
          | {:error,
             :timeout | :invalid_json | :expected_json_object | :duplicate_json_object_key}
  def validate_object(value, %DeadlineBudget{} = budget) when is_binary(value) do
    case DeadlineTask.run(budget, fn -> do_validate_object(value) end) do
      {:ok, result} -> result
      {:error, :timeout} = error -> error
      {:error, {:deadline_task_failed, _reason}} -> {:error, :invalid_json}
    end
  end

  defp do_validate_object(value) do
    case Jason.decode(value, objects: :ordered_objects) do
      {:ok, %OrderedObject{} = object} -> InvocationJSONValidator.validate(object)
      {:ok, _value} -> {:error, :expected_json_object}
      {:error, _reason} -> {:error, :invalid_json}
    end
  end

  @spec encode(term(), DeadlineBudget.t()) ::
          {:ok, binary()} | {:error, :timeout | :not_json_encodable}
  def encode(value, %DeadlineBudget{} = budget) do
    case DeadlineTask.run(budget, fn -> do_encode(value) end) do
      {:ok, result} -> result
      {:error, :timeout} = error -> error
      {:error, {:deadline_task_failed, _reason}} -> {:error, :not_json_encodable}
    end
  end

  defp do_encode(value) do
    with :ok <- InvocationJSONInputValidator.validate(value) do
      case Jason.encode(value, maps: :strict) do
        {:ok, encoded} -> {:ok, encoded}
        {:error, _reason} -> {:error, :not_json_encodable}
      end
    end
  rescue
    _error -> {:error, :not_json_encodable}
  catch
    _kind, _reason -> {:error, :not_json_encodable}
  end
end
