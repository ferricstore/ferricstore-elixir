defmodule FerricStore.HTTP.Transport do
  @moduledoc false

  alias FerricStore.{DeadlineBudget, DeadlineTask}
  alias FerricStore.HTTP.{Envelope, Error, Options, RequestLimit}

  @redirect_statuses [301, 302, 303, 307, 308]
  @max_redirects 10

  def post(%Options{} = config, commands, %DeadlineBudget{} = budget) do
    with :ok <- DeadlineBudget.ensure_active(budget),
         :ok <- batch_limit(config, commands),
         :ok <- RequestLimit.preflight(commands, config.max_request_bytes),
         {:ok, body} <- Envelope.encode_commands(commands),
         :ok <- request_limit(config, body),
         :ok <- DeadlineBudget.ensure_active(budget) do
      run_request(config, body, budget)
    else
      {:error, :timeout} -> Error.timeout()
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> Error.invalid(reason)
    end
  end

  defp run_request(config, body, budget) do
    task = fn ->
      request(config, budget, :post, config.command_url, request_headers(config), body, 0)
    end

    case DeadlineTask.run(budget, task) do
      {:ok, result} -> result
      {:error, :timeout} -> Error.timeout()
      {:error, reason} -> Error.network(reason)
    end
  end

  defp request(config, budget, method, url, headers, body, redirects) do
    with {:ok, timeout} <- DeadlineBudget.request_timeout(budget),
         {:ok, status, response_headers, response_body} <-
           stream(config, method, url, headers, body, timeout) do
      if status in @redirect_statuses do
        redirect(config, budget, status, url, headers, body, response_headers, redirects)
      else
        decode_response(status, response_headers, response_body)
      end
    else
      {:error, :timeout} -> Error.timeout()
      {:error, %Error{} = error} -> {:error, error}
      {:error, reason} -> Error.network(reason)
    end
  end

  defp stream(config, method, url, headers, body, timeout) do
    request = Finch.build(method, url, headers, body)
    initial = %{status: nil, headers: [], body: [], size: 0, error: nil}
    opts = [receive_timeout: timeout, pool_timeout: timeout, request_timeout: timeout]

    stream_with_retry(request, config, initial, opts, 100)
  end

  defp stream_with_retry(request, config, initial, opts, attempts) do
    case Finch.stream_while(request, config.pool, initial, &stream_event(&1, &2, config), opts) do
      {:ok, %{error: nil, status: status} = acc} when is_integer(status) ->
        {:ok, status, acc.headers, acc.body |> Enum.reverse() |> IO.iodata_to_binary()}

      {:ok, %{error: reason}} ->
        {:error, reason}

      {:error, reason} ->
        retry_stream(request, config, initial, opts, attempts, reason)

      {:error, reason, _acc} ->
        retry_stream(request, config, initial, opts, attempts, reason)
    end
  end

  defp retry_stream(request, config, initial, opts, attempts, %Finch.Error{reason: reason})
       when attempts > 0 and reason in [:connection_not_ready, :pool_not_available] do
    receive do
    after
      5 -> stream_with_retry(request, config, initial, opts, attempts - 1)
    end
  end

  defp retry_stream(_request, _config, _initial, _opts, _attempts, reason),
    do: {:error, reason}

  defp stream_event({:status, status}, acc, _config),
    do: {:cont, %{acc | status: status}}

  defp stream_event({:headers, headers}, acc, config) do
    case content_length(headers) do
      length when is_integer(length) and length > config.max_response_bytes ->
        {:halt, %{acc | headers: headers, error: :response_too_large}}

      _unknown_or_allowed ->
        {:cont, %{acc | headers: headers}}
    end
  end

  defp stream_event({:data, data}, acc, config) do
    size = acc.size + byte_size(data)

    if size > config.max_response_bytes,
      do: {:halt, %{acc | error: :response_too_large}},
      else: {:cont, %{acc | body: [data | acc.body], size: size}}
  end

  defp stream_event(_event, acc, _config), do: {:cont, acc}

  defp redirect(_config, _budget, _status, _url, _headers, _body, _response, redirects)
       when redirects >= @max_redirects,
       do: Error.invalid(:too_many_redirects)

  defp redirect(config, budget, status, url, headers, body, response_headers, redirects) do
    with {:ok, location} <- location(response_headers),
         {:ok, target} <- merge_location(url, location) do
      {method, headers, body} = redirect_request(status, headers, body)
      request(config, budget, method, target, headers, body, redirects + 1)
    else
      {:error, reason} -> Error.invalid(reason)
    end
  end

  defp redirect_request(status, headers, _body) when status in [301, 302, 303] do
    headers =
      Enum.reject(headers, fn {name, _value} -> name in ["content-type", "content-length"] end)

    {:get, headers, nil}
  end

  defp redirect_request(_status, headers, body), do: {:post, headers, body}

  defp decode_response(status, headers, body) do
    case Envelope.decode(body) do
      {:ok, envelope} -> {:ok, status, headers, envelope}
      {:error, _reason} when status < 200 or status >= 300 -> {:ok, status, headers, %{}}
      {:error, reason} -> Error.invalid(reason)
    end
  end

  defp request_headers(config) do
    [{"content-type", "application/json"}, {"accept", "application/json"} | config.headers]
    |> Enum.uniq_by(&elem(&1, 0))
  end

  defp location(headers) do
    case List.keyfind(headers, "location", 0) do
      {_, location} when is_binary(location) and location != "" -> {:ok, location}
      _missing -> {:error, :redirect_without_location}
    end
  end

  defp merge_location(current, location) do
    case URI.merge(current, location) do
      %URI{scheme: scheme, host: host} = uri
      when scheme in ["http", "https"] and is_binary(host) ->
        {:ok, URI.to_string(uri)}

      _invalid ->
        {:error, :invalid_redirect_location}
    end
  rescue
    ArgumentError -> {:error, :invalid_redirect_location}
  end

  defp content_length(headers) do
    with {_, value} <- List.keyfind(headers, "content-length", 0),
         {length, ""} <- Integer.parse(value),
         true <- length >= 0 do
      length
    else
      _invalid -> nil
    end
  end

  defp batch_limit(config, commands) do
    if length(commands) <= config.max_batch_items,
      do: :ok,
      else: {:error, :batch_too_large}
  end

  defp request_limit(config, body) do
    if byte_size(body) <= config.max_request_bytes,
      do: :ok,
      else: {:error, :request_too_large}
  end
end
