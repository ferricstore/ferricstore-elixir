defmodule FerricStore.SDK.KV.StructuredResponse do
  @moduledoc false

  alias FerricStore.SDK.KV.RateLimitResponse

  @spec rate_limit(
          {:ok, term()} | {:error, term()},
          pos_integer(),
          pos_integer(),
          pos_integer()
        ) ::
          {:ok, term()} | {:error, term()}
  def rate_limit(
        {:ok, value},
        maximum,
        window_ms,
        increment
      ) do
    case RateLimitResponse.validate(value, maximum, window_ms, increment) do
      :ok -> {:ok, value}
      :error -> invalid(:ratelimit_add)
    end
  end

  def rate_limit({:error, _reason} = error, _maximum, _window_ms, _increment), do: error

  @spec fetch_or_compute({:ok, term()} | {:error, term()}) ::
          {:ok, term()} | {:error, term()}
  def fetch_or_compute({:ok, ["hit", value] = response}) when is_binary(value),
    do: {:ok, response}

  def fetch_or_compute({:ok, ["compute", hint, token] = value})
      when is_binary(hint) and is_binary(token) and token != "",
      do: {:ok, value}

  def fetch_or_compute({:ok, _value}), do: invalid(:fetch_or_compute)
  def fetch_or_compute({:error, _reason} = error), do: error

  defp invalid(operation),
    do: {:error, {:invalid_kv_response, %{operation: operation, reason: :unexpected_shape}}}
end
