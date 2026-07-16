defmodule FerricStore.Protocol.PipelineCommand do
  @moduledoc false

  alias FerricStore.Protocol.{CommandSpec, PipelineRawCommand}
  alias FerricStore.Types

  @command_exec_opcode CommandSpec.fetch!(:command_exec).opcode
  @supported_fields MapSet.new(["opcode", "body", "lane_id", "request_id"])

  @spec validate(term(), non_neg_integer()) :: :ok | {:error, term()}
  def validate(command, index) when is_integer(index) and index >= 0 do
    case fields(command, index) do
      {:ok, _fields} -> :ok
      {:error, _reason} = error -> error
    end
  end

  @spec normalize(term(), pos_integer(), non_neg_integer()) :: {:ok, map()} | {:error, term()}
  def normalize(command, request_id, index)
      when is_integer(request_id) and request_id > 0 and is_integer(index) and index >= 0 do
    case fields(command, index) do
      {:ok, {:raw, name, args}} ->
        {:ok,
         %{
           "opcode" => @command_exec_opcode,
           "lane_id" => 1,
           "request_id" => request_id,
           "body" => %{"command" => name, "args" => args}
         }}

      {:ok, {:typed, opcode, body, lane_id, command_request_id}} ->
        {:ok,
         %{
           "opcode" => opcode,
           "lane_id" => lane_id,
           "request_id" => command_request_id || request_id,
           "body" => body
         }}

      {:error, _reason} = error ->
        error
    end
  end

  defp fields([name | args], index), do: PipelineRawCommand.fields(name, args, index)
  defp fields(command, index) when is_map(command), do: typed_fields(command, index)
  defp fields(_command, index), do: error(index, :expected_nonempty_list_or_typed_map)

  defp typed_fields(command, index) when map_size(command) <= 4 do
    with {:ok, fields} <- normalize_keys(command, index),
         :ok <- supported_fields(fields, index),
         {:ok, opcode} <- unsigned_field(fields, "opcode", 0xFFFF, :invalid_opcode, index),
         :ok <- data_opcode(opcode, index),
         {:ok, body} <- body_field(fields, index),
         {:ok, lane_id} <-
           optional_unsigned(fields, "lane_id", 1, 0xFFFF_FFFF, :invalid_lane_id, index),
         {:ok, request_id} <- optional_request_id(fields, index) do
      {:ok, {:typed, opcode, body, lane_id, request_id}}
    end
  end

  defp typed_fields(_command, index), do: error(index, :unsupported_fields)

  defp data_opcode(opcode, index) do
    if CommandSpec.control_lane?(opcode), do: error(index, :control_opcode), else: :ok
  end

  defp normalize_keys(command, index) do
    case Types.normalize_map_keys_result(command) do
      {:ok, fields} -> {:ok, fields}
      {:error, {:duplicate_normalized_map_key, _key}} -> error(index, :duplicate_field)
      {:error, {:invalid_map_key, _key}} -> error(index, :unsupported_fields)
    end
  end

  defp supported_fields(fields, index) do
    if Enum.all?(Map.keys(fields), &MapSet.member?(@supported_fields, &1)),
      do: :ok,
      else: error(index, :unsupported_fields)
  end

  defp body_field(fields, index) do
    case Map.fetch(fields, "body") do
      {:ok, body} when is_map(body) -> {:ok, body}
      _missing_or_invalid -> error(index, :invalid_body)
    end
  end

  defp optional_unsigned(fields, field, default, max, reason, index) do
    case Map.fetch(fields, field) do
      :error -> {:ok, default}
      {:ok, value} when is_integer(value) and value >= 0 and value <= max -> {:ok, value}
      {:ok, _value} -> error(index, reason)
    end
  end

  defp optional_request_id(fields, index) do
    case Map.fetch(fields, "request_id") do
      :error ->
        {:ok, nil}

      {:ok, value} when is_integer(value) and value >= 0 and value <= 0xFFFF_FFFF_FFFF_FFFF ->
        {:ok, value}

      {:ok, _value} ->
        error(index, :invalid_request_id)
    end
  end

  defp unsigned_field(fields, field, max, reason, index) do
    case Map.fetch(fields, field) do
      {:ok, value} when is_integer(value) and value >= 0 and value <= max -> {:ok, value}
      _missing_or_invalid -> error(index, reason)
    end
  end

  defp error(index, reason),
    do: {:error, {:invalid_pipeline_command, %{index: index, reason: reason}}}
end
