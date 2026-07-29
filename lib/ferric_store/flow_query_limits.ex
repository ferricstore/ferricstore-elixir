defmodule FerricStore.FlowQueryLimits do
  @moduledoc false

  @max_query_bytes 16 * 1_024
  @max_records 100
  @max_projection_fields 32

  @spec max_query_bytes() :: pos_integer()
  def max_query_bytes, do: @max_query_bytes

  @spec max_records() :: pos_integer()
  def max_records, do: @max_records

  @spec max_projection_fields() :: pos_integer()
  def max_projection_fields, do: @max_projection_fields
end
