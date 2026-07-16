defmodule FerricStore.Transport.EventDispatcherClient do
  @moduledoc false

  alias FerricStore.Timeout
  alias FerricStore.Transport.EventDispatcherProtocol

  @force_stop_timeout 100

  @spec dispatch(pid(), term(), timeout()) :: EventDispatcherProtocol.dispatch_result()
  def dispatch(dispatcher, event, timeout) do
    if Timeout.valid?(timeout), do: do_dispatch(dispatcher, event, timeout), else: :dropped
  end

  defp do_dispatch(dispatcher, event, timeout) do
    request_ref = make_ref()
    reply_alias = Process.alias()
    monitor = Process.monitor(dispatcher)

    send(
      dispatcher,
      {EventDispatcherProtocol, :prepare_dispatch, self(), reply_alias, request_ref, event}
    )

    receive do
      {EventDispatcherProtocol, :reply, ^request_ref, result} ->
        Process.unalias(reply_alias)
        Process.demonitor(monitor, [:flush])
        maybe_commit_dispatch(dispatcher, request_ref, result)
        result

      {:DOWN, ^monitor, :process, ^dispatcher, _reason} ->
        Process.unalias(reply_alias)
        :dropped
    after
      timeout ->
        Process.unalias(reply_alias)
        send(dispatcher, {EventDispatcherProtocol, :cancel_dispatch, request_ref})
        Process.demonitor(monitor, [:flush])
        :dropped
    end
  end

  @spec request(pid(), term(), timeout(), term()) :: term()
  def request(dispatcher, request, timeout, unavailable) do
    if Timeout.valid?(timeout),
      do: do_request(dispatcher, request, timeout, unavailable),
      else: unavailable
  end

  defp do_request(dispatcher, request, timeout, unavailable) do
    request_ref = make_ref()
    reply_alias = Process.alias()
    monitor = Process.monitor(dispatcher)
    send(dispatcher, {EventDispatcherProtocol, :request, reply_alias, request_ref, request})

    receive do
      {EventDispatcherProtocol, :reply, ^request_ref, result} ->
        Process.unalias(reply_alias)
        Process.demonitor(monitor, [:flush])
        result

      {:DOWN, ^monitor, :process, ^dispatcher, _reason} ->
        Process.unalias(reply_alias)
        unavailable
    after
      timeout ->
        Process.unalias(reply_alias)
        Process.demonitor(monitor, [:flush])
        unavailable
    end
  end

  @spec stop(pid(), timeout()) :: :ok
  def stop(dispatcher, timeout) do
    if Timeout.valid?(timeout), do: do_stop(dispatcher, timeout), else: :ok
  end

  defp do_stop(dispatcher, timeout) do
    request_ref = make_ref()
    monitor = Process.monitor(dispatcher)
    send(dispatcher, {EventDispatcherProtocol, :stop, self(), request_ref})

    receive do
      {EventDispatcherProtocol, :stopped, ^request_ref} ->
        Process.demonitor(monitor, [:flush])
        :ok

      {:DOWN, ^monitor, :process, ^dispatcher, _reason} ->
        :ok
    after
      timeout -> force_stop(dispatcher, monitor, request_ref)
    end
  end

  defp maybe_commit_dispatch(_dispatcher, _request_ref, :dropped), do: :ok

  defp maybe_commit_dispatch(dispatcher, request_ref, _admitted) do
    send(dispatcher, {EventDispatcherProtocol, :commit_dispatch, request_ref})
    :ok
  end

  defp force_stop(dispatcher, monitor, request_ref) do
    send(dispatcher, {EventDispatcherProtocol, :force_stop, self(), request_ref})

    receive do
      {EventDispatcherProtocol, :stopped, ^request_ref} ->
        Process.demonitor(monitor, [:flush])
        :ok

      {:DOWN, ^monitor, :process, ^dispatcher, _reason} ->
        :ok
    after
      @force_stop_timeout ->
        Process.exit(dispatcher, :kill)
        Process.demonitor(monitor, [:flush])
        :ok
    end
  end
end
