defmodule FerricStore.SDK.Native.RequestRegistry do
  @moduledoc false

  defstruct requests: %{}, connection_tags: %{}, async_tags: %{}

  @type tag :: reference()
  @type request :: map()
  @type t :: %__MODULE__{
          requests: %{optional(tag()) => request()},
          connection_tags: %{optional(pid()) => MapSet.t(tag())},
          async_tags: %{optional(reference()) => tag()}
        }

  @spec size(t()) :: non_neg_integer()
  def size(%__MODULE__{requests: requests}), do: map_size(requests)

  @spec request_tag(request()) :: tag()
  def request_tag(_request), do: make_ref()

  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{requests: requests}), do: map_size(requests) == 0

  @spec get(t(), tag()) :: request() | nil
  def get(%__MODULE__{requests: requests}, tag), do: Map.get(requests, tag)

  @spec fetch(t(), tag()) :: {:ok, request()} | :error
  def fetch(%__MODULE__{requests: requests}, tag), do: Map.fetch(requests, tag)

  @spec member?(t(), tag()) :: boolean()
  def member?(%__MODULE__{requests: requests}, tag), do: Map.has_key?(requests, tag)

  @spec fetch_async(t(), pid(), reference()) :: {:ok, tag(), request()} | :error
  def fetch_async(%__MODULE__{} = registry, owner, ref)
      when is_pid(owner) and is_reference(ref) do
    with {:ok, tag} <- Map.fetch(registry.async_tags, ref),
         {:ok, %{from: {:async, ^owner, ^ref}} = request} <- Map.fetch(registry.requests, tag) do
      {:ok, tag, request}
    else
      _missing_or_not_owner -> :error
    end
  end

  @spec put(t(), tag(), request()) :: t()
  def put(%__MODULE__{} = registry, tag, request) do
    previous = Map.get(registry.requests, tag)

    registry
    |> delete_connection_tag(tag, previous)
    |> delete_async_tag(tag, previous)
    |> put_request(tag, request)
    |> put_connection_tag(tag, request)
    |> put_async_tag(tag, request)
  end

  @spec update!(t(), tag(), (request() -> request())) :: t()
  def update!(%__MODULE__{} = registry, tag, updater) when is_function(updater, 1) do
    request = registry.requests |> Map.fetch!(tag) |> updater.()
    put(registry, tag, request)
  end

  @spec update(t(), tag(), (request() -> request())) :: t()
  def update(%__MODULE__{} = registry, tag, updater) when is_function(updater, 1) do
    case Map.fetch(registry.requests, tag) do
      {:ok, request} -> put(registry, tag, updater.(request))
      :error -> registry
    end
  end

  @spec pop(t(), tag()) :: {request() | nil, t()}
  def pop(%__MODULE__{} = registry, tag) do
    case Map.pop(registry.requests, tag) do
      {nil, _requests} ->
        {nil, registry}

      {request, requests} ->
        registry = %{registry | requests: requests}

        registry =
          registry
          |> delete_connection_tag(tag, request)
          |> delete_async_tag(tag, request)

        {request, registry}
    end
  end

  @spec pop_many(t(), MapSet.t(tag())) :: {t(), [{tag(), request()}]}
  def pop_many(%__MODULE__{} = registry, tags) do
    Enum.reduce(tags, {registry, []}, fn tag, {registry, removed} ->
      case pop(registry, tag) do
        {nil, registry} -> {registry, removed}
        {request, registry} -> {registry, [{tag, request} | removed]}
      end
    end)
  end

  @spec requests(t()) :: %{optional(tag()) => request()}
  def requests(%__MODULE__{requests: requests}), do: requests

  @spec connection_tags(t(), pid()) :: MapSet.t(tag())
  def connection_tags(%__MODULE__{connection_tags: connection_tags}, connection)
      when is_pid(connection),
      do: Map.get(connection_tags, connection, MapSet.new())

  defp put_request(registry, tag, request),
    do: %{registry | requests: Map.put(registry.requests, tag, request)}

  defp put_connection_tag(registry, tag, %{conn: connection}) when is_pid(connection) do
    tags = registry.connection_tags |> Map.get(connection, MapSet.new()) |> MapSet.put(tag)
    %{registry | connection_tags: Map.put(registry.connection_tags, connection, tags)}
  end

  defp put_connection_tag(registry, _tag, _request), do: registry

  defp delete_connection_tag(registry, tag, %{conn: connection}) when is_pid(connection) do
    case Map.get(registry.connection_tags, connection) do
      nil ->
        registry

      tags ->
        tags = MapSet.delete(tags, tag)

        connection_tags =
          if MapSet.size(tags) == 0,
            do: Map.delete(registry.connection_tags, connection),
            else: Map.put(registry.connection_tags, connection, tags)

        %{registry | connection_tags: connection_tags}
    end
  end

  defp delete_connection_tag(registry, _tag, _request), do: registry

  defp put_async_tag(registry, tag, %{from: {:async, _owner, ref}})
       when is_reference(ref),
       do: %{registry | async_tags: Map.put(registry.async_tags, ref, tag)}

  defp put_async_tag(registry, _tag, _request), do: registry

  defp delete_async_tag(registry, tag, %{from: {:async, _owner, ref}})
       when is_reference(ref) do
    case Map.fetch(registry.async_tags, ref) do
      {:ok, ^tag} -> %{registry | async_tags: Map.delete(registry.async_tags, ref)}
      _missing_or_newer -> registry
    end
  end

  defp delete_async_tag(registry, _tag, _request), do: registry
end
