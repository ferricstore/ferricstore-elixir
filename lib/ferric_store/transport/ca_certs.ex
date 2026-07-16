defmodule FerricStore.Transport.CACerts do
  @moduledoc false

  @enforce_keys [:certificates, :fingerprint]
  defstruct [:certificates, :fingerprint]

  @max_certificates 1_024
  @max_certificate_bytes 1_048_576
  @max_total_bytes 16 * 1_048_576

  @type t :: %__MODULE__{certificates: list(), fingerprint: binary()}

  @spec prepare(t() | list()) :: t()
  def prepare(%__MODULE__{} = prepared), do: prepared

  def prepare(certificates) when is_list(certificates) do
    if valid_certificates?(certificates) do
      %__MODULE__{
        certificates: certificates,
        fingerprint: :crypto.hash(:sha256, :erlang.term_to_binary(certificates))
      }
    else
      raise ArgumentError, "invalid CA certificate collection"
    end
  end

  @spec certificates(t() | list()) :: list()
  def certificates(%__MODULE__{certificates: certificates}), do: certificates
  def certificates(certificates) when is_list(certificates), do: certificates

  @spec fingerprint(t()) :: binary()
  def fingerprint(%__MODULE__{fingerprint: fingerprint}), do: fingerprint

  @spec valid?(term()) :: boolean()
  def valid?(%__MODULE__{certificates: certificates, fingerprint: fingerprint}) do
    valid_certificates?(certificates) and is_binary(fingerprint) and
      fingerprint == certificate_fingerprint(certificates)
  end

  def valid?(certificates), do: valid_certificates?(certificates)

  defp valid_certificates?(certificates), do: valid_certificates?(certificates, 0, 0)

  defp valid_certificates?([], _count, _bytes), do: true

  defp valid_certificates?([certificate | certificates], count, bytes)
       when is_binary(certificate) and count < @max_certificates do
    certificate_bytes = byte_size(certificate)
    total_bytes = bytes + certificate_bytes

    certificate_bytes > 0 and certificate_bytes <= @max_certificate_bytes and
      total_bytes <= @max_total_bytes and
      valid_certificates?(certificates, count + 1, total_bytes)
  end

  defp valid_certificates?(_improper_or_invalid, _count, _bytes), do: false

  defp certificate_fingerprint(certificates),
    do: :crypto.hash(:sha256, :erlang.term_to_binary(certificates))
end
