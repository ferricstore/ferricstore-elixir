defmodule FerricStore.Protocol.ValueDomain do
  @moduledoc false

  @min_signed_64 -9_223_372_036_854_775_808
  @max_signed_64 9_223_372_036_854_775_807
  @max_unsigned_64 18_446_744_073_709_551_615

  defguard is_signed_64_integer(value)
           when is_integer(value) and value >= @min_signed_64 and value <= @max_signed_64

  defguard is_unsigned_64_integer(value)
           when is_integer(value) and value > @max_signed_64 and value <= @max_unsigned_64

  defguard is_native_score(value)
           when is_float(value) or
                  (is_integer(value) and value >= @min_signed_64 and value <= @max_signed_64 and
                     trunc(value * 1.0) == value)
end
