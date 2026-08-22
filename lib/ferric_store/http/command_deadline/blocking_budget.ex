defmodule FerricStore.HTTP.CommandDeadline.BlockingBudget do
  @moduledoc false

  alias FerricStore.Protocol.{CommandName, CommandSpec, PipelineRequest}
  alias FerricStore.{Timeout, Types}

  @command_exec_opcode CommandSpec.fetch!(:command_exec).opcode
  @pipeline_opcode CommandSpec.fetch!(:pipeline).opcode
  @max_command_exec_depth 8
  @max_finite_timeout Timeout.max_finite()

  @type t :: {:extend, non_neg_integer()} | :disable_default

  @spec for_message(term()) :: t()
  def for_message({:request, @pipeline_opcode, %PipelineRequest{commands: commands}, _context}),
    do: pipeline_budget(commands)

  def for_message({:request, @command_exec_opcode, payload, _context}),
    do: command_exec_budget(payload)

  def for_message({:command, @command_exec_opcode, _key, payload, _context}),
    do: command_exec_budget(payload)

  def for_message({:async_request, _owner, _ref, @pipeline_opcode, payload, _context}),
    do: for_message({:request, @pipeline_opcode, payload, nil})

  def for_message({:async_request, _owner, _ref, @command_exec_opcode, payload, _context}),
    do: command_exec_budget(payload)

  def for_message({:async_command, _owner, _ref, @command_exec_opcode, _key, payload, _context}),
    do: command_exec_budget(payload)

  def for_message(_message), do: {:extend, 0}

  defp command_exec_budget(payload, depth \\ 0)

  defp command_exec_budget(payload, depth) when is_map(payload) do
    command = Types.get(payload, "command")
    args = Types.get(payload, "args")

    if is_binary(command) and is_list(args),
      do: command_budget([command | args], depth),
      else: {:extend, 0}
  rescue
    ArgumentError -> {:extend, 0}
  end

  defp command_exec_budget(_payload, _depth), do: {:extend, 0}

  defp pipeline_budget(commands) when is_list(commands) do
    Enum.reduce_while(commands, {:extend, 0}, fn command, accumulated ->
      case merge_budget(accumulated, command_budget(command, 0)) do
        :disable_default = disabled -> {:halt, disabled}
        budget -> {:cont, budget}
      end
    end)
  end

  defp pipeline_budget(_commands), do: {:extend, 0}

  defp command_budget(_command, depth) when depth >= @max_command_exec_depth,
    do: :disable_default

  defp command_budget([command | args], depth) when is_binary(command) and is_list(args) do
    case CommandName.normalize(command) do
      {:ok, "COMMAND_EXEC"} -> command_budget(args, depth + 1)
      {:ok, normalized} -> named_command_budget(normalized, args)
      {:error, _reason} -> {:extend, 0}
    end
  end

  defp command_budget(command, depth) when is_map(command) do
    opcode = Types.get(command, "opcode")
    body = Types.get(command, "body")

    if opcode == @command_exec_opcode and is_map(body) do
      command_exec_budget(body, depth + 1)
    else
      {:extend, 0}
    end
  rescue
    ArgumentError -> {:extend, 0}
  end

  defp command_budget(_command, _depth), do: {:extend, 0}

  defp named_command_budget(command, args)
       when command in ["BLPOP", "BRPOP", "BLMOVE", "BRPOPLPUSH", "BZPOPMIN", "BZPOPMAX"] do
    args |> List.last() |> duration_budget(1_000)
  end

  defp named_command_budget(command, [timeout | _args]) when command in ["BLMPOP", "BZMPOP"],
    do: duration_budget(timeout, 1_000)

  defp named_command_budget(command, args) when command in ["XREAD", "XREADGROUP"],
    do: stream_budget(command, args)

  defp named_command_budget(_command, _args), do: {:extend, 0}

  defp stream_budget("XREADGROUP", [group, _name, _consumer | args]) do
    case CommandName.normalize(group) do
      {:ok, "GROUP"} -> stream_option_budget(args, true)
      _invalid -> {:extend, 0}
    end
  end

  defp stream_budget("XREAD", args), do: stream_option_budget(args, false)
  defp stream_budget(_command, _args), do: {:extend, 0}

  defp stream_option_budget([], _group?), do: {:extend, 0}

  defp stream_option_budget([option | args], group?) do
    case CommandName.normalize(option) do
      {:ok, "STREAMS"} ->
        {:extend, 0}

      {:ok, "COUNT"} ->
        skip_stream_option(args, group?)

      {:ok, "BLOCK"} ->
        case args do
          [timeout | _rest] -> duration_budget(timeout, 1)
          _missing -> {:extend, 0}
        end

      {:ok, "NOACK"} when group? ->
        stream_option_budget(args, group?)

      _unknown_or_invalid ->
        {:extend, 0}
    end
  end

  defp skip_stream_option([_value | rest], group?), do: stream_option_budget(rest, group?)
  defp skip_stream_option(_missing, _group?), do: {:extend, 0}

  defp duration_budget(nil, _unit_ms), do: {:extend, 0}

  defp duration_budget(value, unit_ms) do
    case non_negative_number(value) do
      {:ok, 0} ->
        :disable_default

      {:ok, number} when number > 0 ->
        extension_budget(number, unit_ms)

      :error ->
        {:extend, 0}
    end
  end

  defp non_negative_number(value) when is_integer(value) and value >= 0, do: {:ok, value}

  defp non_negative_number(value) when is_float(value) do
    if value >= 0 and value <= @max_finite_timeout, do: {:ok, value}, else: :error
  end

  defp non_negative_number(value) when is_binary(value) do
    case Integer.parse(value) do
      {number, ""} when number >= 0 -> {:ok, number}
      _not_integer -> parse_float(value)
    end
  end

  defp non_negative_number(_value), do: :error

  defp parse_float(value) do
    case Float.parse(value) do
      {number, ""} when number >= 0 and number <= @max_finite_timeout -> {:ok, number}
      _invalid -> :error
    end
  end

  defp extension_budget(number, unit_ms) do
    max = @max_finite_timeout

    if number > max / unit_ms do
      :disable_default
    else
      {:extend, trunc(number * unit_ms)}
    end
  end

  defp merge_budget(:disable_default, _budget), do: :disable_default
  defp merge_budget(_budget, :disable_default), do: :disable_default

  defp merge_budget({:extend, first}, {:extend, second}) do
    if second > @max_finite_timeout - first,
      do: :disable_default,
      else: {:extend, first + second}
  end
end
