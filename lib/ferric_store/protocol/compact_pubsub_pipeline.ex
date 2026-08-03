defmodule FerricStore.Protocol.CompactPubSubPipeline do
  @moduledoc false

  alias FerricStore.Protocol.CommandName

  @request_tag 0x94
  @publish_mode 35

  @spec build(list(), keyword(), boolean()) :: {:ok, {:custom_payload, iodata()}} | :fallback
  def build(commands, options, advertised?)
      when is_list(commands) and is_list(options) and is_boolean(advertised?) do
    if advertised? and compact_return?(options) and
         not Keyword.has_key?(options, :request_context) do
      case encode_items(commands, []) do
        {:ok, items} ->
          {:ok,
           {:custom_payload,
            [<<@request_tag, @publish_mode, length(commands)::unsigned-32>> | items]}}

        :fallback ->
          :fallback
      end
    else
      :fallback
    end
  end

  def build(_commands, _options, _advertised?), do: :fallback

  defp compact_return?(options), do: Keyword.get(options, :return) in [:compact, "compact"]

  defp encode_items([], acc), do: {:ok, Enum.reverse(acc)}

  defp encode_items([[name, channel, message] | commands], acc)
       when is_binary(channel) and is_binary(message) do
    if publish?(name) do
      encode_items(commands, [[binary(channel), binary(message)] | acc])
    else
      :fallback
    end
  end

  defp encode_items(_commands, _acc), do: :fallback

  defp publish?("PUBLISH"), do: true
  defp publish?(name), do: match?({:ok, "PUBLISH"}, CommandName.normalize(name))

  defp binary(value), do: [<<byte_size(value)::unsigned-32>>, value]
end
