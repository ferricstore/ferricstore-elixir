defmodule FerricStore.SDK.Native.ClientCredentialValidationTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK

  @seeds [{"127.0.0.1", 1}]

  test "rejects credentials beyond the server contract without echoing them" do
    oversized_username = String.duplicate("u", 1_025)
    oversized_password = String.duplicate("p", 4_097)

    assert {:error,
            {:invalid_client_option, :username, %{reason: :too_large, bytes: 1_025, limit: 1_024}}} =
             SDK.start_link(
               seeds: @seeds,
               username: oversized_username,
               password: "secret"
             )

    assert {:error,
            {:invalid_client_option, :password, %{reason: :too_large, bytes: 4_097, limit: 4_096}}} =
             SDK.start_link(seeds: @seeds, password: oversized_password)
  end

  test "rejects invalid UTF-8 usernames before authentication" do
    assert {:error, {:invalid_client_option, :username, %{reason: :invalid_utf8}}} =
             SDK.start_link(seeds: @seeds, username: <<0xFF>>, password: "secret")
  end
end
