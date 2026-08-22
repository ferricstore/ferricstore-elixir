defmodule FerricStore.HTTPClientTest do
  use ExUnit.Case, async: false

  alias FerricStore.ClientIdentity
  alias FerricStore.HTTP.Client, as: HTTPClient
  alias FerricStore.HTTP.Command
  alias FerricStore.HTTP.Envelope
  alias FerricStore.HTTP.Finch.HTTP2
  alias FerricStore.Protocol.CommandSpec
  alias FerricStore.SDK

  setup do
    bypass = Bypass.open()
    {:ok, bypass: bypass, url: "http://127.0.0.1:#{bypass.port}"}
  end

  test "URL selection keeps the command API and binary envelope unchanged", context do
    bytes = <<0, 1, 255>>

    Bypass.expect_once(context.bypass, "POST", "/v1/commands", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      envelope = Jason.decode!(body)

      assert envelope == %{
               "encoding" => "ferricstore-json-v1",
               "commands" => [
                 ["ECHO", %{"$ferricstore_bytes" => Base.encode64(bytes)}]
               ]
             }

      response = success_envelope(%{"$ferricstore_bytes" => Base.encode64(bytes)})
      Plug.Conn.send_resp(conn, 200, Jason.encode!(response))
    end)

    assert {:ok, client} = SDK.from_url(context.url)
    assert ClientIdentity.type(client) == :http
    assert bytes == FerricStore.command(client, "ECHO", [bytes])
    assert :ok = SDK.close(client)
  end

  test "typed SDK commands use validated structured descriptors", context do
    Bypass.expect_once(context.bypass, "POST", "/v1/commands", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      [descriptor] = Jason.decode!(body)["commands"]
      assert descriptor["command"] == "SET"
      assert descriptor["opcode"] == CommandSpec.fetch!(:set).opcode
      assert decode_map(descriptor["payload"])["key"] == "key"
      Plug.Conn.send_resp(conn, 200, Jason.encode!(success_envelope("OK")))
    end)

    assert {:ok, client} = SDK.from_url(context.url)
    assert {:ok, :ok} = SDK.set(client, "key", "value")
    assert :ok = SDK.close(client)
  end

  test "HTTP payloads normalize atom values and map keys like the native codec" do
    descriptor = %{
      "command" => "FLOW.SCHEDULE.CREATE",
      "opcode" => CommandSpec.fetch!(:flow_schedule_create).opcode,
      "payload" => %{kind: :interval, target: %{id_prefix: "run", type: :scheduled}}
    }

    assert {:ok, body} = Envelope.encode_commands([descriptor])
    [encoded] = Jason.decode!(body)["commands"]
    payload = decode_map(encoded["payload"])
    target = decode_map(payload["target"])
    assert payload["kind"] == "interval"
    assert target == %{"id_prefix" => "run", "type" => "scheduled"}

    assert {:error, {:invalid_http_value, :duplicate_map_key}} =
             Envelope.encode_commands([
               %{descriptor | "payload" => %{:kind => "first", "kind" => "second"}}
             ])
  end

  test "one SDK pipeline is one HTTP request with ordered item outcomes", context do
    Bypass.expect_once(context.bypass, "POST", "/v1/commands", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      assert length(Jason.decode!(body)["commands"]) == 3

      response = %{
        "encoding" => "ferricstore-json-v1",
        "results" => [
          %{"status" => "ok", "value" => "PONG"},
          %{"status" => "error", "error" => %{"code" => "noperm", "message" => "denied"}},
          %{"status" => "ok", "value" => 3}
        ]
      }

      Plug.Conn.send_resp(conn, 200, Jason.encode!(response))
    end)

    assert {:ok, client} = SDK.from_url(context.url)

    assert [["ok", "PONG"], ["error", %{"code" => "noperm", "message" => "denied"}], ["ok", 3]] =
             FerricStore.pipeline(client, [["PING"], ["GET", "secret"], ["INCR", "counter"]])

    assert :ok = SDK.close(client)
  end

  test "bearer headers survive a cross-origin redirect", context do
    target = Bypass.open()

    Bypass.expect_once(context.bypass, "POST", "/v1/commands", fn conn ->
      conn
      |> Plug.Conn.put_resp_header("location", "http://127.0.0.1:#{target.port}/redirected")
      |> Plug.Conn.send_resp(307, "")
    end)

    Bypass.expect_once(target, "POST", "/redirected", fn conn ->
      assert Plug.Conn.get_req_header(conn, "authorization") == ["Bearer secret"]
      assert Plug.Conn.get_req_header(conn, "x-trace") == ["trace-1"]
      Plug.Conn.send_resp(conn, 200, Jason.encode!(success_envelope("PONG")))
    end)

    assert {:ok, client} =
             SDK.from_url(context.url, bearer_token: "secret", headers: %{"x-trace" => "trace-1"})

    assert {:ok, "PONG"} = SDK.ping(client)
    assert :ok = SDK.close(client)
  end

  test "HTTP/2 is explicit and uses the multiplexed pool", context do
    assert {:ok, client} = SDK.from_url(context.url, http2: true)
    assert %{http2: true, pool: HTTP2} = HTTPClient.config(client)
    assert :ok = SDK.close(client)
  end

  test "the complete typed command catalog has an HTTP disposition" do
    dispositions =
      Map.new(CommandSpec.all(), fn spec ->
        {spec.name, Command.disposition(spec.name, spec.opcode)}
      end)

    assert map_size(dispositions) == length(CommandSpec.all())
    assert Enum.all?(dispositions, fn {_name, value} -> value in [:supported, :native_only] end)
    assert dispositions["SET"] == :supported
    assert dispositions["PIPELINE"] == :native_only
  end

  defp success_envelope(value) do
    %{
      "encoding" => "ferricstore-json-v1",
      "results" => [%{"status" => "ok", "value" => value}]
    }
  end

  defp decode_map(%{"$ferricstore_map" => pairs}) do
    Map.new(pairs, fn [key, value] -> {key, value} end)
  end
end
