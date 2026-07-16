defmodule FerricStore.Transport.EventDispatcherWorker do
  @moduledoc false

  alias FerricStore.Transport.EventDispatcherProtocol

  @spec start(pid(), (term() -> term())) :: pid()
  def start(dispatcher, handler) when is_pid(dispatcher) and is_function(handler, 1),
    do: spawn_link(fn -> loop(dispatcher, handler) end)

  @spec stop(pid() | nil) :: :ok
  def stop(nil), do: :ok

  def stop(worker) when is_pid(worker) do
    Process.exit(worker, :kill)
    :ok
  end

  defp loop(dispatcher, handler) do
    receive do
      {EventDispatcherProtocol, :invoke, ^dispatcher, token, event} ->
        outcome = invoke(handler, event)
        send(dispatcher, {EventDispatcherProtocol, :worker_done, self(), token, outcome})
        loop(dispatcher, handler)
    end
  end

  defp invoke(handler, event) do
    handler.(event)
    :ok
  rescue
    error -> {:error, {:exception, error}}
  catch
    kind, reason -> {:error, {kind, reason}}
  end
end
