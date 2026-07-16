defmodule FerricStore.SDK.KV.ScoreResponseParser do
  @moduledoc false

  @max_score_bytes 128

  def parse(value, error) when is_binary(value) and byte_size(value) <= @max_score_bytes do
    case Float.parse(value) do
      {score, ""} -> {:ok, score}
      _invalid -> {:error, error}
    end
  end

  def parse(_value, error), do: {:error, error}
end
