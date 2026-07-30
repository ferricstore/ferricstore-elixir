defmodule FerricStore.SDK.Native.PipelinePreparer do
  @moduledoc false

  alias FerricStore.BoundedList
  alias FerricStore.Protocol
  alias FerricStore.Protocol.{CommandSpec, CompactStreamPipeline, PipelineRequest}

  @pipeline_opcode CommandSpec.fetch!(:pipeline).opcode

  @spec prepare(non_neg_integer(), term(), {non_neg_integer(), boolean()}) ::
          {:ok, term()} | {:error, term()}
  def prepare(opcode, payload, {limit, compact_stream_xadd}),
    do: prepare(opcode, payload, limit, compact_stream_xadd)

  @spec prepare(non_neg_integer(), term(), non_neg_integer()) ::
          {:ok, term()} | {:error, term()}
  def prepare(opcode, payload, limit), do: prepare(opcode, payload, limit, false)

  @spec prepare(non_neg_integer(), term(), non_neg_integer(), boolean()) ::
          {:ok, term()} | {:error, term()}
  def prepare(
        @pipeline_opcode,
        %PipelineRequest{commands: commands, command_count: command_count, options: options},
        limit,
        compact_stream_xadd
      ) do
    with :ok <- admit_command_count(command_count, limit) do
      case CompactStreamPipeline.build(commands, options, compact_stream_xadd) do
        {:ok, payload} -> {:ok, payload}
        :fallback -> Protocol.pipeline_payload_result(commands, options)
      end
    end
  end

  def prepare(@pipeline_opcode, %{"commands" => commands} = payload, limit, _compact_stream_xadd)
      when is_list(commands) do
    with :ok <- admit_commands(commands, limit), do: {:ok, payload}
  end

  def prepare(@pipeline_opcode, %{commands: commands} = payload, limit, _compact_stream_xadd)
      when is_list(commands) do
    with :ok <- admit_commands(commands, limit), do: {:ok, payload}
  end

  def prepare(_opcode, %PipelineRequest{}, _limit, _compact_stream_xadd),
    do: {:error, :invalid_pipeline_opcode}

  def prepare(_opcode, payload, _limit, _compact_stream_xadd), do: {:ok, payload}

  defp admit_commands(commands, limit) do
    case BoundedList.count(commands, limit) do
      {:ok, _count} ->
        :ok

      {:error, {:limit_exceeded, observed}} ->
        {:error, {:pipeline_too_large, %{items: observed, limit: limit}}}

      {:error, :improper_list} ->
        {:error, {:invalid_pipeline, :improper_list}}
    end
  end

  defp admit_command_count(count, limit) when count <= limit, do: :ok

  defp admit_command_count(_count, limit),
    do: {:error, {:pipeline_too_large, %{items: limit + 1, limit: limit}}}
end
