defmodule FerricStore.SDK.Native.ConnectionClient do
  @moduledoc false

  alias FerricStore.SDK.Native.ConnectionTimers

  @default_timeout 5_000

  @spec close(pid()) :: :ok
  def close(pid) when is_pid(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid, :normal), else: :ok
  catch
    :exit, _reason -> :ok
  end

  @spec request(pid(), non_neg_integer(), term(), non_neg_integer(), timeout()) ::
          {:ok, term()} | {:error, term()}
  def request(pid, opcode, payload, lane_id, timeout \\ @default_timeout) do
    deadline = ConnectionTimers.request_deadline(timeout)

    GenServer.call(
      pid,
      {:request, opcode, payload, lane_id, timeout, deadline},
      ConnectionTimers.call_timeout(timeout)
    )
  end

  @spec complete_bootstrap(pid(), term(), timeout()) :: :ok
  def complete_bootstrap(pid, startup, timeout \\ @default_timeout) when is_pid(pid) do
    GenServer.call(
      pid,
      {:complete_bootstrap, startup},
      ConnectionTimers.call_timeout(timeout)
    )
  end

  @spec capacity(pid(), timeout()) :: %{
          required(:max_in_flight) => non_neg_integer(),
          required(:max_in_flight_per_lane) => non_neg_integer()
        }
  def capacity(pid, timeout \\ @default_timeout) when is_pid(pid),
    do: GenServer.call(pid, :capacity, ConnectionTimers.call_timeout(timeout))

  @spec async_request(
          pid(),
          pid(),
          reference(),
          non_neg_integer(),
          term(),
          non_neg_integer(),
          timeout()
        ) :: :ok
  def async_request(pid, reply_to, tag, opcode, payload, lane_id, timeout \\ @default_timeout)
      when is_pid(reply_to) and is_reference(tag) do
    deadline = ConnectionTimers.request_deadline(timeout)

    GenServer.cast(
      pid,
      {:async_request, reply_to, tag, opcode, payload, lane_id, timeout, deadline}
    )
  end

  @spec cancel(pid(), pid(), reference(), timeout()) ::
          :ok | {:error, :connection_closed | :timeout}
  def cancel(pid, reply_to, tag, timeout \\ @default_timeout)
      when is_pid(pid) and is_pid(reply_to) and is_reference(tag) do
    GenServer.call(pid, {:cancel, reply_to, tag}, ConnectionTimers.call_timeout(timeout))
  catch
    :exit, {:timeout, _call} -> {:error, :timeout}
    :exit, _reason -> {:error, :connection_closed}
  end

  @spec cancel_async(pid(), pid(), reference()) :: :ok
  def cancel_async(pid, reply_to, tag)
      when is_pid(pid) and is_pid(reply_to) and is_reference(tag) do
    GenServer.cast(pid, {:cancel, reply_to, tag})
  end

  @spec drain(pid()) :: :ok
  def drain(pid) when is_pid(pid), do: GenServer.cast(pid, :drain)

  @spec abort(pid(), term()) :: :ok
  def abort(pid, reason) when is_pid(pid), do: GenServer.cast(pid, {:abort, reason})
end
