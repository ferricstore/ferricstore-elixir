defmodule FerricStore.SDK.Native.RequestRetrySafety do
  @moduledoc false

  alias FerricStore.Protocol.{CommandName, Opcodes}
  alias FerricStore.RequestContext
  alias FerricStore.Types

  @command_exec Opcodes.command_exec()
  @flow_policy_set Opcodes.flow_policy_set()

  @spec classify(non_neg_integer(), term(), RequestContext.t()) :: RequestContext.t()
  def classify(@flow_policy_set, payload, %RequestContext{} = context) when is_map(payload) do
    if is_nil(Types.get(payload, "expected_generation")),
      do: context,
      else: RequestContext.disable_automatic_retry(context)
  end

  def classify(@command_exec, payload, %RequestContext{} = context) when is_map(payload) do
    if raw_policy_generation_cas?(payload),
      do: RequestContext.disable_automatic_retry(context),
      else: context
  end

  def classify(_opcode, _payload, %RequestContext{} = context), do: context

  @spec classify(RequestContext.t(), boolean()) :: RequestContext.t()
  def classify(%RequestContext{} = context, true),
    do: RequestContext.disable_automatic_retry(context)

  def classify(%RequestContext{} = context, false), do: context

  defp raw_policy_generation_cas?(payload) do
    case {CommandName.normalize(Types.get(payload, "command")), Types.get(payload, "args")} do
      {{:ok, "FLOW.POLICY.SET"}, [_type | args]} when is_list(args) ->
        Enum.any?(args, &expected_generation?/1)

      _other ->
        false
    end
  end

  defp expected_generation?(arg),
    do: match?({:ok, "EXPECTED_GENERATION"}, CommandName.normalize(arg))
end
