defmodule FerricStore.HTTPClientTest do
  use ExUnit.Case, async: false

  alias FerricStore.ClientIdentity
  alias FerricStore.HTTP.Client, as: HTTPClient
  alias FerricStore.HTTP.Command
  alias FerricStore.HTTP.Envelope
  alias FerricStore.HTTP.Error
  alias FerricStore.HTTP.Finch.HTTP2
  alias FerricStore.Protocol.CommandSpec
  alias FerricStore.SDK
  alias FerricStore.Timeout

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

  test "generic HTTP ping preserves native payload compatibility", context do
    expected_messages = ["PONG", "custom"]
    {:ok, counter} = Agent.start_link(fn -> 0 end)

    Bypass.expect(context.bypass, "POST", "/v1/commands", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      index = Agent.get_and_update(counter, &{&1, &1 + 1})
      expected = Enum.fetch!(expected_messages, index)
      assert Jason.decode!(body)["commands"] == [["PING", expected]]
      Plug.Conn.send_resp(conn, 200, Jason.encode!(success_envelope(expected)))
    end)

    assert {:ok, client} = SDK.from_url(context.url)
    assert {:ok, "PONG"} = SDK.request(client, :ping, false)
    assert {:ok, "custom"} = SDK.request(client, :ping, %{message: "custom"})
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

    assert {:error, {:invalid_http_value, :improper_list}} =
             Envelope.encode_commands([
               %{descriptor | "payload" => %{"items" => ["value" | :invalid_tail]}}
             ])
  end

  test "malformed typed response markers fail closed without raising" do
    assert {:error, {:invalid_http_response, :invalid_base64}} =
             Envelope.decode(~s({"$ferricstore_bytes":1}))

    duplicate =
      Jason.encode!(%{
        "$ferricstore_map" => [
          ["key", "first"],
          ["key", "second"]
        ]
      })

    assert {:error, {:invalid_http_response, :duplicate_map_key}} = Envelope.decode(duplicate)
  end

  test "HTTP options reject malformed keywords and ambiguous or invalid headers", context do
    assert {:error, {:invalid_http_url, :port}} =
             SDK.from_url("http://127.0.0.1:99999")

    assert {:error, {:invalid_http_options, [:not_an_option]}} =
             SDK.from_url(context.url, [:not_an_option])

    assert {:error, {:invalid_http_options, {:duplicate_options, [:bearer_token]}}} =
             SDK.from_url(context.url, bearer_token: "first", bearer_token: "second")

    assert {:error, {:invalid_http_header, {:duplicate, "authorization"}}} =
             SDK.from_url(context.url,
               headers: [{"Authorization", "Bearer first"}, {"authorization", "Bearer second"}]
             )

    assert {:error, {:invalid_http_headers, :invalid_tail}} =
             SDK.from_url(context.url, headers: [{"x-trace", "trace"} | :invalid_tail])

    assert {:error, {:invalid_http_header, "bad header"}} =
             SDK.from_url(context.url, headers: [{"bad header", "value"}])

    assert {:error, {:invalid_http_header, {:reserved, "content-length"}}} =
             SDK.from_url(context.url, headers: [{"content-length", "0"}])

    assert {:error, {:invalid_http_credentials, :bearer_token}} =
             SDK.from_url(context.url, bearer_token: "")

    unsafe_timeout = Timeout.max_finite() + 1

    assert {:error, {:invalid_http_option, :timeout, ^unsafe_timeout}} =
             SDK.from_url(context.url, timeout: unsafe_timeout)
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

  test "finite blocking waits extend the implicit HTTP timeout", context do
    Bypass.expect_once(context.bypass, "POST", "/v1/commands", fn conn ->
      Process.sleep(100)
      Plug.Conn.send_resp(conn, 200, Jason.encode!(success_envelope(nil)))
    end)

    assert {:ok, client} = SDK.from_url(context.url, timeout: 50)
    assert nil == FerricStore.command(client, "BLPOP", ["jobs", "0.3"])
    assert :ok = SDK.close(client)
  end

  test "mixed blocking pipelines share one extended HTTP deadline", context do
    Bypass.expect_once(context.bypass, "POST", "/v1/commands", fn conn ->
      Process.sleep(100)

      response = %{
        "encoding" => "ferricstore-json-v1",
        "results" => [
          %{"status" => "ok", "value" => "PONG"},
          %{"status" => "ok", "value" => nil},
          %{"status" => "ok", "value" => nil},
          %{"status" => "ok", "value" => "PONG"}
        ]
      }

      Plug.Conn.send_resp(conn, 200, Jason.encode!(response))
    end)

    assert {:ok, client} = SDK.from_url(context.url, timeout: 30)

    assert [["ok", "PONG"], ["ok", nil], ["ok", nil], ["ok", "PONG"]] =
             FerricStore.pipeline(client, [
               ["PING"],
               ["BLPOP", "jobs", "0.1"],
               ["XREAD", "BLOCK", "100", "STREAMS", "orders", "$"],
               ["PING"]
             ])

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

  test "connection-affine event waits fail locally for HTTP", context do
    assert {:ok, client} = SDK.from_url(context.url)
    assert {:error, {:http_native_only, :await_event}} = SDK.await_event(client, 0)
    assert :ok = SDK.close(client)
  end

  test "a truncated transport response after STEP_CONTINUE is outcome unknown", context do
    owner = self()
    counter = :atomics.new(1, signed: false)

    Bypass.expect(context.bypass, "POST", "/v1/commands", fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      [command] = Jason.decode!(body)["commands"]

      case :atomics.add_get(counter, 1, 1) do
        1 ->
          assert command["command"] == "FLOW.EXTEND_LEASE"

          record = %{
            "id" => "flow-1",
            "partition_key" => "tenant-a",
            "lease_token" => "lease-1",
            "fencing_token" => 7,
            "state" => "running",
            "run_state" => "charge",
            "value_refs" => %{}
          }

          Plug.Conn.send_resp(conn, 200, Jason.encode!(success_envelope(record)))

        2 ->
          assert command["command"] == "FLOW.STEP_CONTINUE"
          send(owner, :commit_request_received)

          Plug.Conn.send_resp(
            conn,
            200,
            ~s({"encoding":"ferricstore-json-v1","results":[)
          )
      end
    end)

    assert {:ok, client} = SDK.from_url(context.url)

    job = %{
      "id" => "flow-1",
      "partition_key" => "tenant-a",
      "lease_token" => "lease-1",
      "fencing_token" => 7,
      "run_state" => "charge"
    }

    assert {:error,
            %FerricStore.Flow.DurableMutationOutcomeUnknownError{
              cause: %Error{delivery: :unknown}
            }} =
             FerricStore.Flow.step(client, job,
               name: "charge-customer:v1",
               run: fn -> "charged" end,
               to_state: "schedule_warning"
             )

    assert_receive :commit_request_received
    assert :atomics.get(counter, 1) == 2
    assert :ok = SDK.close(client)
  end

  test "invalid UTF-8 command names fail locally without crashing", context do
    invalid = <<0xFF, 0xFE>>
    assert {:ok, client} = SDK.from_url(context.url)

    assert {:error, %FerricStore.Error{raw: {:invalid_command, %{reason: :invalid_utf8}}}} =
             FerricStore.command(client, invalid)

    message =
      {:request, CommandSpec.fetch!(:command_exec).opcode, %{"command" => invalid, "args" => []},
       nil}

    assert {:error,
            %Error{
              error_code: "native_only",
              reason: {:http_native_only, :invalid_command_exec}
            }} = Command.execute(HTTPClient.config(client), message, 5_000)

    assert Process.alive?(client)
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
