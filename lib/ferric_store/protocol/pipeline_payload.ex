defmodule FerricStore.Protocol.PipelinePayload do
  @moduledoc false

  alias FerricStore.FailureFormatter
  alias FerricStore.Protocol.{PipelineCommand, RequestContextCodec}
  alias FerricStore.RequestLimits

  @max_commands RequestLimits.max_command_items()

  def build(commands, opts) do
    case normalize(commands) do
      {:ok, protocol_commands} -> payload(protocol_commands, opts)
      {:error, reason} -> raise ArgumentError, inspect(reason)
    end
  end

  def build_result(commands, opts) do
    case normalize(commands) do
      {:ok, protocol_commands} -> safely(fn -> payload(protocol_commands, opts) end)
      {:error, _reason} = error -> error
    end
  end

  defp normalize(commands) do
    with :ok <- admit(commands, 0), do: normalize_commands(commands, 1, 0, [])
  end

  defp admit([], _count), do: :ok

  defp admit([_command | _commands], @max_commands),
    do: {:error, {:pipeline_too_large, %{items: @max_commands + 1, limit: @max_commands}}}

  defp admit([_command | commands], count), do: admit(commands, count + 1)

  defp admit(_improper_tail, count),
    do: {:error, {:invalid_pipeline_command, %{index: count, reason: :improper_command_list}}}

  defp normalize_commands([], _request_id, _index, acc), do: {:ok, Enum.reverse(acc)}

  defp normalize_commands([command | commands], request_id, index, acc) do
    with {:ok, command} <- PipelineCommand.normalize(command, request_id, index) do
      normalize_commands(commands, request_id + 1, index + 1, [command | acc])
    end
  end

  defp payload(commands, opts) do
    %{"atomicity" => "none", "commands" => commands}
    |> put_return(opts)
    |> RequestContextCodec.put(opts)
  end

  defp put_return(payload, opts) do
    case Keyword.get(opts, :return) do
      mode when mode in [:compact, "compact"] -> Map.put(payload, "return", "compact")
      mode when mode in [:pairs, "pairs"] -> Map.put(payload, "return", "pairs")
      _other -> payload
    end
  end

  defp safely(builder) do
    {:ok, builder.()}
  rescue
    error ->
      {:error,
       {:invalid_command,
        FailureFormatter.exception_message(error, "command construction failed")}}
  catch
    kind, reason -> {:error, {:invalid_command, FailureFormatter.inspect_term({kind, reason})}}
  end
end
