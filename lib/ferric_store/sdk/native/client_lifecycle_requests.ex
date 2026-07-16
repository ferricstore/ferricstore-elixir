defmodule FerricStore.SDK.Native.ClientLifecycleRequests do
  @moduledoc false

  alias FerricStore.{ClientIdentity, ClientShutdown, RouteKey, Timeout}

  alias FerricStore.SDK.Native.{
    ClientOptions,
    ClientRequestAdmission,
    ClientSupervisor,
    CoordinatorCall,
    EventInbox,
    EventSubscriptionAdmission,
    TopologyRefreshRequests
  }

  @default_timeout 5_000

  def start_link(opts) do
    with :ok <- ClientRequestAdmission.validate_client_options(opts),
         do: ClientSupervisor.start_link(opts)
  end

  def close(client, timeout) when is_pid(client) do
    if Timeout.valid?(timeout) do
      case ClientIdentity.type(client) do
        :topology_aware -> ClientShutdown.stop(client, timeout)
        :unknown -> {:error, {:invalid_client, :unknown}}
        :dead -> :ok
      end
    else
      ClientShutdown.stop(client, timeout)
    end
  end

  def close(_client, _timeout), do: {:error, {:invalid_client, :not_pid}}

  def cancel_async(client, owner, ref, timeout)
      when is_pid(client) and is_pid(owner) and is_reference(ref) do
    if Timeout.valid?(timeout) do
      case CoordinatorCall.call(client, {:cancel_async, owner, ref}, timeout) do
        {:error, reason} -> {:error, {:cancel_failed, reason}}
        result -> result
      end
    else
      {:error, {:cancel_failed, {:invalid_timeout, timeout}}}
    end
  end

  def from_url(url, opts) do
    with {:ok, opts} <- ClientOptions.merge_url(url, opts), do: start_link(opts)
  end

  def event_subscription(client, action, events, opts)
      when action in [:subscribe, :unsubscribe] do
    with {:ok, subscriber, context} <-
           EventSubscriptionAdmission.prepare(events, opts, @default_timeout) do
      CoordinatorCall.submit(
        client,
        {:event_subscription, action, subscriber, events, context},
        call_timeout(context)
      )
    end
  end

  def await_event(client, timeout), do: EventInbox.await(client, timeout)
  def topology(client), do: CoordinatorCall.call(client, :topology)

  def route(client, key) do
    with {:ok, ^key} <- RouteKey.validate(key),
         do: CoordinatorCall.call(client, {:route, key})
  end

  def refresh_topology(client, timeout), do: TopologyRefreshRequests.submit(client, timeout)

  defp call_timeout(context),
    do: FerricStore.RequestContext.call_timeout(context, @default_timeout)
end
