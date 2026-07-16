defmodule FerricStore.Protocol.CommandPayload do
  @moduledoc false

  alias FerricStore.FailureFormatter
  alias FerricStore.Protocol.{CommandName, RequestContextCodec}

  def build(command, args, opts) do
    case CommandName.normalize(command) do
      {:ok, command} -> payload(command, args, opts)
      {:error, reason} -> raise ArgumentError, "invalid command name: #{reason}"
    end
  end

  def build_result(command, args, opts) do
    case CommandName.normalize(command) do
      {:ok, command} -> safely(fn -> payload(command, args, opts) end)
      {:error, reason} -> {:error, {:invalid_command, %{reason: reason, value: command}}}
    end
  end

  defp payload(command, args, opts) do
    %{"command" => command, "args" => args}
    |> RequestContextCodec.put(opts)
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
