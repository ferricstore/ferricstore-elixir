defmodule FerricStore.Transport.EventDispatcherCallerRegistry do
  @moduledoc false

  defstruct by_reference: %{}, by_monitor: %{}

  @type t :: %__MODULE__{by_reference: map(), by_monitor: map()}

  @spec track(t(), reference(), pid(), pos_integer()) :: t()
  def track(%__MODULE__{} = registry, request_ref, caller, timeout)
      when is_reference(request_ref) and is_pid(caller) and is_integer(timeout) and timeout > 0 do
    registry = release(registry, request_ref)
    monitor = Process.monitor(caller)

    timer =
      Process.send_after(self(), {__MODULE__, :commit_timeout, request_ref, monitor}, timeout)

    %{
      registry
      | by_reference: Map.put(registry.by_reference, request_ref, {monitor, timer}),
        by_monitor: Map.put(registry.by_monitor, monitor, request_ref)
    }
  end

  @spec release(t(), reference()) :: t()
  def release(%__MODULE__{} = registry, request_ref) when is_reference(request_ref) do
    case Map.pop(registry.by_reference, request_ref) do
      {nil, _references} ->
        registry

      {{monitor, timer}, references} ->
        cancel_timer(timer)
        Process.demonitor(monitor, [:flush])

        %{
          registry
          | by_reference: references,
            by_monitor: Map.delete(registry.by_monitor, monitor)
        }
    end
  end

  @spec down(t(), reference()) :: {reference() | nil, t()}
  def down(%__MODULE__{} = registry, monitor) when is_reference(monitor) do
    case Map.pop(registry.by_monitor, monitor) do
      {nil, _monitors} ->
        {nil, registry}

      {request_ref, monitors} ->
        {{^monitor, timer}, references} = Map.pop(registry.by_reference, request_ref)
        cancel_timer(timer)
        {request_ref, %{registry | by_reference: references, by_monitor: monitors}}
    end
  end

  @spec expire(t(), reference(), reference()) :: {boolean(), t()}
  def expire(%__MODULE__{} = registry, request_ref, monitor) do
    case Map.get(registry.by_reference, request_ref) do
      {^monitor, _timer} -> {true, release(registry, request_ref)}
      _missing_or_stale -> {false, registry}
    end
  end

  @spec clear(t()) :: t()
  def clear(%__MODULE__{} = registry) do
    Enum.each(registry.by_reference, fn {_request_ref, {monitor, timer}} ->
      cancel_timer(timer)
      Process.demonitor(monitor, [:flush])
    end)

    %__MODULE__{}
  end

  defp cancel_timer(timer) do
    Process.cancel_timer(timer, async: true, info: false)
    :ok
  end
end
