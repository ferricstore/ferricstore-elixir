defmodule FerricStore.SDK.KV.MGetGroupValidation do
  @moduledoc false

  @spec size_error(non_neg_integer(), non_neg_integer()) :: {:mismatched_mget_response, map()}
  def size_error(expected, actual),
    do: {:mismatched_mget_response, %{expected: expected, actual: actual}}
end
