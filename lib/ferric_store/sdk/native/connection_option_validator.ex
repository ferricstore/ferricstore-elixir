defmodule FerricStore.SDK.Native.ConnectionOptionValidator do
  @moduledoc false

  alias FerricStore.{BoundedList, Timeout}
  alias FerricStore.SDK.Native.EndpointName
  alias FerricStore.Transport.CACerts

  @max_ca_file_bytes 4_096

  @spec valid?(atom(), term()) :: boolean()
  def valid?(:server_name, nil), do: true
  def valid?(:server_name, value), do: EndpointName.valid?(value)
  def valid?(:verify, value), do: optional_boolean?(value)
  def valid?(:tls_verify, value), do: optional_boolean?(value)
  def valid?(:cacertfile, value), do: optional_ca_file?(value)
  def valid?(:cacerts, value), do: optional_cacerts?(value)
  def valid?(:drain_timeout, nil), do: true
  def valid?(:drain_timeout, value), do: Timeout.finite?(value)
  def valid?(:send_timeout, nil), do: true
  def valid?(:send_timeout, value), do: Timeout.finite?(value)
  def valid?(:heartbeat_interval, value), do: optional_positive_timeout?(value)

  def valid?(key, value)
      when key in [:connect_timeout, :server_chunk_timeout, :heartbeat_timeout],
      do: optional_timeout?(value)

  def valid?(_key, value), do: optional_positive_integer?(value)

  defp optional_boolean?(nil), do: true
  defp optional_boolean?(value), do: is_boolean(value)
  defp optional_ca_file?(nil), do: true
  defp optional_ca_file?(false), do: true
  defp optional_ca_file?(value), do: valid_ca_file?(value)

  defp valid_ca_file?(value) when is_binary(value),
    do: value != "" and byte_size(value) <= @max_ca_file_bytes and String.valid?(value)

  defp valid_ca_file?(value) when is_list(value) do
    case BoundedList.count(value, @max_ca_file_bytes) do
      {:ok, _count} -> valid_ca_file_charlist?(value)
      {:error, _reason} -> false
    end
  end

  defp valid_ca_file?(_value), do: false

  defp valid_ca_file_charlist?(value) do
    value |> List.to_string() |> valid_ca_file?()
  rescue
    _error -> false
  end

  defp optional_cacerts?(nil), do: true
  defp optional_cacerts?(false), do: true
  defp optional_cacerts?(value), do: CACerts.valid?(value)
  defp optional_timeout?(nil), do: true
  defp optional_timeout?(value), do: Timeout.valid?(value)
  defp optional_positive_timeout?(nil), do: true
  defp optional_positive_timeout?(value), do: Timeout.positive?(value)
  defp optional_positive_integer?(nil), do: true
  defp optional_positive_integer?(value), do: is_integer(value) and value > 0
end
