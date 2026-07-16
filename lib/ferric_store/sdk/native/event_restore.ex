defmodule FerricStore.SDK.Native.EventRestore do
  @moduledoc false

  defstruct status: :idle,
            attempt: 0,
            token: nil,
            connection: nil,
            timer: nil,
            last_error: nil

  @type status :: :idle | :inflight | :retry_wait
  @type t :: %__MODULE__{
          status: status(),
          attempt: non_neg_integer(),
          token: reference() | nil,
          connection: pid() | nil,
          timer: reference() | nil,
          last_error: term()
        }

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec active?(t()) :: boolean()
  def active?(%__MODULE__{status: status}), do: status != :idle

  @spec next_attempt(t()) :: pos_integer()
  def next_attempt(%__MODULE__{attempt: attempt}), do: attempt + 1

  @spec begin(t(), pid()) :: {reference(), t()}
  def begin(%__MODULE__{status: :idle} = restore, connection) when is_pid(connection) do
    token = make_ref()

    {token,
     %{
       restore
       | status: :inflight,
         attempt: next_attempt(restore),
         token: token,
         connection: connection,
         timer: nil,
         last_error: nil
     }}
  end

  @spec inflight?(t(), reference()) :: boolean()
  def inflight?(%__MODULE__{status: :inflight, token: token}, token), do: true
  def inflight?(%__MODULE__{}, _token), do: false

  @spec retry(t(), pos_integer(), term(), pid(), non_neg_integer()) :: t()
  def retry(%__MODULE__{} = restore, attempt, reason, owner, delay)
      when is_integer(attempt) and attempt > 0 and is_pid(owner) and is_integer(delay) and
             delay >= 0 do
    token = make_ref()
    timer = Process.send_after(owner, {:retry_event_restore, token}, delay)

    %{
      restore
      | status: :retry_wait,
        attempt: attempt,
        token: token,
        connection: nil,
        timer: timer,
        last_error: reason
    }
  end

  @spec activate_retry(t(), reference()) :: {:ok, t()} | :stale
  def activate_retry(%__MODULE__{status: :retry_wait, token: token} = restore, token) do
    {:ok, %{restore | status: :idle, token: nil, timer: nil, connection: nil}}
  end

  def activate_retry(%__MODULE__{}, _token), do: :stale

  @spec reset(t()) :: t()
  def reset(%__MODULE__{} = restore) do
    cancel(restore)
    %__MODULE__{}
  end

  @spec cancel(t()) :: :ok
  def cancel(%__MODULE__{timer: timer}) when is_reference(timer) do
    _result = Process.cancel_timer(timer, async: false, info: false)
    :ok
  end

  def cancel(%__MODULE__{}), do: :ok
end
