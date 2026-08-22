defmodule FerricStore.HTTP.Command do
  @moduledoc false

  alias FerricStore.HTTP.{Error, Response, Transport}
  alias FerricStore.Protocol.{CommandSpec, PipelineCommand, PipelineRequest}

  @pipeline_opcode CommandSpec.fetch!(:pipeline).opcode
  @command_exec_opcode CommandSpec.fetch!(:command_exec).opcode
  @ping_opcode CommandSpec.fetch!(:ping).opcode
  @native_only MapSet.new(~w(
    HELLO AUTH CLIENT.SETNAME CLIENT.INFO ROUTE SHARDS BACKPRESSURE QUIT GOAWAY
    OPTIONS STARTUP WINDOW_UPDATE PIPELINE ROUTE_BATCH EVENT SUBSCRIBE_EVENTS
    UNSUBSCRIBE_EVENTS
  ))
  @session_only MapSet.new(~w(
    AUTH CLIENT HELLO QUIT SELECT MULTI EXEC DISCARD WATCH UNWATCH SUBSCRIBE
    UNSUBSCRIBE PSUBSCRIBE PUNSUBSCRIBE BLPOP BRPOP BLMOVE BLMPOP XREAD XREADGROUP
  ))

  @spec disposition(binary(), non_neg_integer()) :: :supported | :native_only
  def disposition(name, _opcode) when is_binary(name) do
    if MapSet.member?(@native_only, name), do: :native_only, else: :supported
  end

  @spec execute(struct(), term(), timeout()) :: term()
  def execute(config, message, timeout) do
    case prepare(message) do
      {:single, command} -> execute_single(config, command, timeout)
      {:pipeline, commands} -> execute_pipeline(config, commands, timeout)
      {:group, command, indexes} -> execute_group(config, command, indexes, timeout)
      {:error, _reason} = error -> error
    end
  end

  defp prepare({:request, @pipeline_opcode, %PipelineRequest{commands: commands}, _context}),
    do: prepare_pipeline(commands)

  defp prepare({:request, @command_exec_opcode, payload, _context}),
    do: prepare_command_exec(payload)

  defp prepare({:command, @command_exec_opcode, _key, payload, _context}),
    do: prepare_command_exec(payload)

  defp prepare({:request, @ping_opcode, payload, _context}) do
    {:single, ["PING", Map.get(payload, "message", "PONG")]}
  end

  defp prepare({:request, opcode, payload, _context}), do: prepare_typed(opcode, payload)
  defp prepare({:command, opcode, _key, payload, _context}), do: prepare_typed(opcode, payload)

  defp prepare({:command_items, opcode, items, count, _key_fun, builder, _context}) do
    with {:ok, payload} <- build_payload(builder, items),
         {:single, command} <- prepare_typed(opcode, payload) do
      {:group, command, indexes(count)}
    else
      {:error, _reason} = error -> error
    end
  end

  defp prepare(_message), do: unsupported(:request_shape)

  defp prepare_typed(_opcode, {:custom_payload, _body}), do: unsupported(:custom_payload)

  defp prepare_typed(opcode, payload) when is_map(payload) do
    case CommandSpec.fetch(opcode) do
      {:ok, %{name: name}} ->
        if disposition(name, opcode) == :native_only,
          do: unsupported(name),
          else: {:single, %{"command" => name, "opcode" => opcode, "payload" => payload}}

      :error ->
        unsupported({:unknown_opcode, opcode})
    end
  end

  defp prepare_typed(_opcode, _payload), do: unsupported(:invalid_payload)

  defp prepare_command_exec(%{"command" => command, "args" => args})
       when is_binary(command) and is_list(args) do
    normalized = String.upcase(command)

    if raw_native_only?(normalized),
      do: unsupported(normalized),
      else: {:single, [command | args]}
  end

  defp prepare_command_exec(_payload), do: unsupported(:invalid_command_exec)

  defp prepare_pipeline(commands) do
    commands
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {command, index}, {:ok, acc} ->
      case pipeline_command(command, index) do
        {:ok, encoded} -> {:cont, {:ok, [encoded | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, encoded} -> {:pipeline, Enum.reverse(encoded)}
      {:error, _reason} = error -> error
    end
  end

  defp pipeline_command(command, index) do
    with {:ok, normalized} <- PipelineCommand.normalize(command, index + 1, index),
         do: normalized_pipeline_command(normalized)
  end

  defp normalized_pipeline_command(%{"opcode" => @command_exec_opcode, "body" => payload}) do
    case prepare_command_exec(payload) do
      {:single, command} -> {:ok, command}
      {:error, _reason} = error -> error
    end
  end

  defp normalized_pipeline_command(%{"opcode" => opcode, "body" => payload}) do
    case prepare_typed(opcode, payload) do
      {:single, command} -> {:ok, command}
      {:error, _reason} = error -> error
    end
  end

  defp execute_single(config, command, timeout) do
    with {:ok, status, headers, envelope} <- Transport.post(config, [command], timeout),
         {:ok, [result]} <- Response.values(status, envelope, 1, headers) do
      result
    end
  end

  defp execute_pipeline(config, commands, timeout) do
    with {:ok, status, headers, envelope} <- Transport.post(config, commands, timeout),
         {:ok, values} <- Response.values(status, envelope, length(commands), headers) do
      {:ok, Enum.map(values, &pipeline_pair/1)}
    end
  end

  defp execute_group(config, command, indexes, timeout) do
    case execute_single(config, command, timeout) do
      {:ok, value} -> {:ok, [%{indexes: indexes, value: value}]}
      {:error, _reason} = error -> error
    end
  end

  defp pipeline_pair({:ok, value}), do: ["ok", value]
  defp pipeline_pair({:error, error}), do: ["error", error]

  defp indexes(0), do: []
  defp indexes(count), do: Enum.to_list(0..(count - 1))

  defp build_payload(builder, items) do
    {:ok, builder.(items)}
  rescue
    error -> {:error, {:http_payload_builder_failed, error}}
  catch
    kind, reason -> {:error, {:http_payload_builder_failed, {kind, reason}}}
  end

  defp raw_native_only?("CLIENT"), do: true
  defp raw_native_only?(name), do: MapSet.member?(@session_only, name)

  defp unsupported(reason) do
    {:error,
     %Error{
       message: "command requires a persistent native TCP session",
       error_code: "native_only",
       reason: {:http_native_only, reason}
     }}
  end
end
