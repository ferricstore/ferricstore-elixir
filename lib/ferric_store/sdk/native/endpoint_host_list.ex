defmodule FerricStore.SDK.Native.EndpointHostList do
  @moduledoc false

  alias FerricStore.BoundedList
  alias FerricStore.SDK.Native.EndpointName

  @max_hosts 1_024

  @spec valid?(term()) :: boolean()
  def valid?(hosts) when is_list(hosts) do
    case BoundedList.count(hosts, @max_hosts) do
      {:ok, _count} -> valid_hosts?(hosts)
      {:error, _reason} -> false
    end
  end

  def valid?(_hosts), do: false

  defp valid_hosts?([]), do: true
  defp valid_hosts?([host | hosts]), do: valid_host?(host) and valid_hosts?(hosts)
  defp valid_hosts?(_improper_tail), do: false

  defp valid_host?(host) when is_binary(host), do: EndpointName.valid?(host)

  defp valid_host?(host) when is_atom(host) and host not in [nil, true, false],
    do: host |> Atom.to_string() |> EndpointName.valid?()

  defp valid_host?(_host), do: false
end
