defmodule FerricStore.HTTP.ClientAdmission do
  @moduledoc false

  alias FerricStore.SDK.Native.AdmissionGate

  @enforce_keys [:gate]
  defstruct [:gate, leases: %{}, owner_monitors: %{}]

  @type t :: %__MODULE__{
          gate: AdmissionGate.t(),
          leases: %{reference() => %{monitor: reference(), owner: pid()}},
          owner_monitors: %{reference() => reference()}
        }

  @spec new(pos_integer()) :: t()
  def new(limit), do: %__MODULE__{gate: AdmissionGate.new(limit)}

  @spec reserve(pid(), timeout()) :: {:ok, reference()} | {:error, term()}
  def reserve(client, timeout) do
    lease = make_ref()
    reserve(client, timeout, lease)
  end

  @spec acquire(t(), reference(), pid()) :: {:ok, t()} | {:error, term()}
  def acquire(%__MODULE__{} = admission, lease, owner) do
    if Map.has_key?(admission.leases, lease) do
      {:error, {:http_unsupported, :duplicate_admission_lease}}
    else
      acquire_new(admission, lease, owner)
    end
  end

  @spec release(t(), reference(), boolean()) :: t()
  def release(%__MODULE__{} = admission, lease, demonitor? \\ true) do
    case Map.pop(admission.leases, lease) do
      {%{monitor: monitor}, leases} ->
        if demonitor?, do: Process.demonitor(monitor, [:flush])
        AdmissionGate.release(admission.gate)

        %{
          admission
          | leases: leases,
            owner_monitors: Map.delete(admission.owner_monitors, monitor)
        }

      {nil, _leases} ->
        admission
    end
  end

  @spec owner_lease(t(), reference()) :: reference() | nil
  def owner_lease(%__MODULE__{} = admission, monitor),
    do: Map.get(admission.owner_monitors, monitor)

  @spec call_error(term()) :: {:error, term()}
  def call_error(reason), do: {:error, call_reason(reason)}

  @spec call_reason(term()) :: term()
  def call_reason({:timeout, {GenServer, :call, _request}}), do: :timeout

  def call_reason({reason, {GenServer, :call, _request}})
      when reason in [:noproc, :normal, :shutdown],
      do: :client_closed

  def call_reason({{:shutdown, _detail}, {GenServer, :call, _request}}), do: :client_closed
  def call_reason(reason) when reason in [:noproc, :normal, :shutdown], do: :client_closed
  def call_reason({:shutdown, _detail}), do: :client_closed
  def call_reason(reason), do: {:client_unavailable, reason}

  defp reserve(client, timeout, lease) do
    case GenServer.call(client, {:reserve_submission, lease}, timeout) do
      {:ok, ^lease} = admitted -> admitted
      {:error, _reason} = error -> error
    end
  catch
    :exit, reason ->
      GenServer.cast(client, {:release_submission, lease})
      call_error(reason)
  end

  defp acquire_new(admission, lease, owner) do
    case AdmissionGate.acquire(admission.gate) do
      :ok ->
        monitor = Process.monitor(owner)

        admission =
          admission
          |> put_in([Access.key(:leases), lease], %{owner: owner, monitor: monitor})
          |> put_in([Access.key(:owner_monitors), monitor], lease)

        {:ok, admission}

      {:error, _reason} = error ->
        error
    end
  end
end
