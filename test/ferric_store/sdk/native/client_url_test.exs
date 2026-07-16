defmodule FerricStore.SDK.Native.ClientURLTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK
  alias FerricStore.Test.NativeServer

  test "from_url authenticates with percent-decoded URL credentials" do
    {:ok, server} = NativeServer.start_link(owner: self())
    port = NativeServer.port(server)

    assert {:ok, client} =
             SDK.from_url("ferric://service%20user:p%40ss%3Aword@127.0.0.1:#{port}")

    assert_receive {:native_server_request,
                    %{
                      opcode: 0x000C,
                      payload: %{
                        "client_name" => "ferricstore-elixir-sdk",
                        "compact_flow_responses" => true,
                        "compression" => "none",
                        "driver_name" => "ferricstore-elixir-sdk"
                      }
                    }}

    assert_receive {:native_server_request,
                    %{
                      opcode: 0x0002,
                      payload: %{
                        "username" => "service user",
                        "password" => "p@ss:word"
                      }
                    }}

    assert_receive {:native_server_request, %{opcode: 0x0007}}
    assert :ok = SDK.close(client)
  end

  test "explicit credentials override URL credentials" do
    {:ok, server} = NativeServer.start_link(owner: self())
    port = NativeServer.port(server)

    assert {:ok, client} =
             SDK.from_url("ferric://url-user:url-pass@127.0.0.1:#{port}",
               username: "option-user",
               password: "option-pass"
             )

    assert_receive {:native_server_request, %{opcode: 0x000C}}

    assert_receive {:native_server_request,
                    %{
                      opcode: 0x0002,
                      payload: %{
                        "username" => "option-user",
                        "password" => "option-pass"
                      }
                    }}

    assert :ok = SDK.close(client)
  end

  test "password-only URLs authenticate the default user" do
    {:ok, server} = NativeServer.start_link(owner: self())
    port = NativeServer.port(server)

    assert {:ok, client} = SDK.from_url("ferric://:secret@127.0.0.1:#{port}")
    assert_receive {:native_server_request, %{opcode: 0x000C}}

    assert_receive {:native_server_request,
                    %{
                      opcode: 0x0002,
                      payload: %{"username" => "default", "password" => "secret"}
                    }}

    assert :ok = SDK.close(client)
  end
end
