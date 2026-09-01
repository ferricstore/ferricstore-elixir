defmodule FerricStore.HTTP.Response do
  @moduledoc false

  alias FerricStore.HTTP.Error

  @encoding "ferricstore-json-v1"

  @spec values(non_neg_integer(), map(), non_neg_integer(), [{binary(), binary()}]) ::
          {:ok, [term()]} | {:error, Error.t()}
  def values(status, envelope, expected, headers) do
    cond do
      status < 200 or status >= 300 -> {:error, top_error(status, envelope, headers)}
      envelope["encoding"] not in [nil, @encoding] -> invalid(:encoding)
      not is_list(envelope["results"]) -> invalid(:results)
      length(envelope["results"]) != expected -> invalid(:result_count)
      true -> decode_results(envelope["results"])
    end
  end

  defp decode_results(results) do
    results
    |> Enum.reduce_while({:ok, []}, fn
      %{"status" => "ok", "value" => value}, {:ok, acc} ->
        {:cont, {:ok, [{:ok, value} | acc]}}

      %{"status" => "error", "error" => error}, {:ok, acc} when is_map(error) ->
        {:cont, {:ok, [{:error, error} | acc]}}

      _invalid, _acc ->
        {:halt, invalid(:result_item)}
    end)
    |> reverse_ok()
  end

  defp top_error(status, envelope, headers) do
    details = if is_map(envelope["error"]), do: envelope["error"], else: %{}
    code = if is_binary(details["code"]), do: details["code"]
    message = if is_binary(details["message"]), do: details["message"]
    reported_safe_to_retry = details["safe_to_retry"] == true
    delivery = delivery(status, code, reported_safe_to_retry)
    safe_to_retry = reported_safe_to_retry and delivery == :rejected

    %Error{
      message: message || "HTTP command request failed with status #{status}",
      status_code: status,
      error_code: code,
      retry_after_ms: retry_after(headers),
      details: details,
      reason: {:http_status, status},
      retryable: status in [408, 425, 429] or status >= 500,
      safe_to_retry: safe_to_retry,
      delivery: delivery
    }
  end

  defp retry_after(headers) do
    with {_, value} <- List.keyfind(headers, "retry-after", 0),
         {seconds, ""} <- Integer.parse(value),
         true <- seconds >= 0 do
      seconds * 1_000
    else
      _invalid -> nil
    end
  end

  defp invalid(reason) do
    {:error,
     %Error{
       message: "invalid HTTP command response",
       reason: {:invalid_http_response, reason},
       safe_to_retry: false,
       delivery: :unknown
     }}
  end

  defp delivery(408, _code, _safe_to_retry), do: :unknown
  defp delivery(_status, "request_timeout", _safe_to_retry), do: :unknown
  defp delivery(_status, _code, true), do: :rejected

  defp delivery(status, _code, _safe_to_retry)
       when status in [400, 401, 403, 404, 405, 406, 411, 413, 414, 415, 422, 426, 431],
       do: :rejected

  defp delivery(_status, code, _safe_to_retry)
       when code in [
              "auth",
              "unauthenticated",
              "unauthorized",
              "noperm",
              "forbidden",
              "bad_request",
              "invalid_command",
              "invalid_request",
              "not_found",
              "flow_not_found",
              "stale_lease",
              "wrong_state",
              "conflict",
              "request_too_large"
            ],
       do: :rejected

  defp delivery(_status, _code, _safe_to_retry), do: :unknown

  defp reverse_ok({:ok, values}), do: {:ok, Enum.reverse(values)}
  defp reverse_ok({:error, _reason} = error), do: error
end
