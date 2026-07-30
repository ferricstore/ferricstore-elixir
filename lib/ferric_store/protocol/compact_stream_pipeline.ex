defmodule FerricStore.Protocol.CompactStreamPipeline do
  @moduledoc false

  alias FerricStore.Protocol.CommandName

  @request_tag 0x94
  @xadd_auto_mode 34
  @max_field_pairs 0xFFFF

  @spec build(list(), keyword(), boolean()) :: {:ok, {:custom_payload, iodata()}} | :fallback
  def build(commands, options, advertised?)
      when is_list(commands) and is_list(options) and is_boolean(advertised?) do
    if advertised? and compact_return?(options) and
         not Keyword.has_key?(options, :request_context) do
      case encode_items(commands, []) do
        {:ok, items} ->
          {:ok,
           {:custom_payload,
            [<<@request_tag, @xadd_auto_mode, length(commands)::unsigned-32>> | items]}}

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

  defp encode_items([[name, key, id | fields] | commands], acc)
       when is_binary(key) and is_binary(id) do
    with true <- xadd?(name) and id == "*",
         {:ok, pair_count, encoded_fields} <- encode_fields(fields, 0, []) do
      item = [binary(key), <<pair_count::unsigned-16>>, encoded_fields]
      encode_items(commands, [item | acc])
    else
      _unsupported -> :fallback
    end
  end

  defp encode_items(_commands, _acc), do: :fallback

  defp xadd?("XADD"), do: true
  defp xadd?(name), do: match?({:ok, "XADD"}, CommandName.normalize(name))

  defp encode_fields([], 0, _acc), do: :fallback
  defp encode_fields([], count, acc), do: {:ok, count, Enum.reverse(acc)}

  defp encode_fields([field, value | fields], count, acc)
       when is_binary(field) and is_binary(value) and count < @max_field_pairs do
    encode_fields(fields, count + 1, [[binary(field), binary(value)] | acc])
  end

  defp encode_fields(_fields, _count, _acc), do: :fallback

  defp binary(value), do: [<<byte_size(value)::unsigned-32>>, value]
end
