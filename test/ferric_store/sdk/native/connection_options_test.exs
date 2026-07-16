defmodule FerricStore.SDK.Native.ConnectionOptionsTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.ConnectionOptions

  test "max_request_bytes reads only the bounded request-size policy" do
    assert ConnectionOptions.max_request_bytes(%{}) == 16 * 1024 * 1024
    assert ConnectionOptions.max_request_bytes(max_request_bytes: 1_024) == 1_024
    assert ConnectionOptions.max_request_bytes(%{"max_request_bytes" => 2_048}) == 2_048
    assert ConnectionOptions.max_request_bytes(%{max_request_bytes: 0}) == 16 * 1024 * 1024
  end
end
