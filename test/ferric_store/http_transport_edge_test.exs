defmodule FerricStore.HTTP2TestHandler do
  @moduledoc false

  def init(request, owner) do
    {:ok, _body, request} = :cowboy_req.read_body(request)
    send(owner, {:http_version, :cowboy_req.version(request)})
    send(owner, {:http_authorization, :cowboy_req.header("authorization", request)})

    response = %{
      "encoding" => "ferricstore-json-v1",
      "results" => [%{"status" => "ok", "value" => "PONG"}]
    }

    request =
      :cowboy_req.reply(
        200,
        %{"content-type" => "application/json"},
        Jason.encode!(response),
        request
      )

    {:ok, request, owner}
  end
end

defmodule FerricStore.HTTPSlowTestHandler do
  @moduledoc false

  def init(request, counter) do
    request_number = Agent.get_and_update(counter, &{&1, &1 + 1})
    if request_number == 1, do: Process.sleep(150)
    {:ok, _body, request} = :cowboy_req.read_body(request)

    response = %{
      "encoding" => "ferricstore-json-v1",
      "results" => [%{"status" => "ok", "value" => "PONG"}]
    }

    request = :cowboy_req.reply(200, %{}, Jason.encode!(response), request)
    {:ok, request, counter}
  end
end

defmodule FerricStore.HTTPAdmissionTestHandler do
  @moduledoc false

  def init(request, %{counter: counter, owner: owner, tag: tag}) do
    request_number = :atomics.add_get(counter, 1, 1)

    if request_number == 1 do
      send(owner, {tag, :request_started})
      Process.sleep(200)
    end

    {:ok, _body, request} = :cowboy_req.read_body(request)

    response = %{
      "encoding" => "ferricstore-json-v1",
      "results" => [%{"status" => "ok", "value" => "PONG"}]
    }

    request = :cowboy_req.reply(200, %{}, Jason.encode!(response), request)
    {:ok, request, owner}
  end
end

defmodule FerricStore.HTTPDeadlineTestHandler do
  @moduledoc false

  def init(request, counter) do
    if :atomics.add_get(counter, 1, 1) == 2, do: Process.sleep(100)
    {:ok, _body, request} = :cowboy_req.read_body(request)

    response = %{
      "encoding" => "ferricstore-json-v1",
      "results" => [%{"status" => "ok", "value" => "PONG"}]
    }

    request = :cowboy_req.reply(200, %{}, Jason.encode!(response), request)
    {:ok, request, counter}
  end
end

defmodule FerricStore.HTTPKeepAliveTestHandler do
  @moduledoc false

  def init(request, owner) do
    send(owner, {:http_peer, :cowboy_req.peer(request)})
    {:ok, _body, request} = :cowboy_req.read_body(request)

    response = %{
      "encoding" => "ferricstore-json-v1",
      "results" => [%{"status" => "ok", "value" => "PONG"}]
    }

    request = :cowboy_req.reply(200, %{}, Jason.encode!(response), request)
    {:ok, request, owner}
  end
end

defmodule FerricStore.HTTPTestTLS do
  @moduledoc false

  def files do
    root_key = rsa_key()
    root_identifier = key_identifier(root_key)

    root =
      :public_key.pkix_test_root_cert(
        ~c"FerricStore SDK test CA",
        options(root_key, identifier_extensions(root_identifier))
      )

    peer_key = rsa_key()
    peer_identifier = key_identifier(peer_key)

    config =
      :public_key.pkix_test_data(%{
        root: root,
        intermediates: [],
        peer:
          options(peer_key, [
            {:Extension, {2, 5, 29, 17}, false, [iPAddress: <<127, 0, 0, 1>>]},
            subject_identifier(peer_identifier),
            authority_identifier(root_identifier)
          ])
      })

    directory =
      Path.join(System.tmp_dir!(), "ferricstore_sdk_tls_#{System.unique_integer([:positive])}")

    certfile = Path.join(directory, "server-cert.pem")
    keyfile = Path.join(directory, "server-key.pem")
    certificate = Keyword.fetch!(config, :cert)
    {key_type, key_der} = Keyword.fetch!(config, :key)
    File.mkdir_p!(directory)
    File.write!(certfile, :public_key.pem_encode([{:Certificate, certificate, :not_encrypted}]))
    File.write!(keyfile, :public_key.pem_encode([{key_type, key_der, :not_encrypted}]))
    %{certfile: certfile, directory: directory, keyfile: keyfile}
  end

  defp options(key, extensions), do: [digest: :sha256, key: key, extensions: extensions]
  defp rsa_key, do: :public_key.generate_key({:rsa, 2_048, 65_537})

  defp key_identifier(private_key) do
    public_key = {:RSAPublicKey, elem(private_key, 2), elem(private_key, 3)}
    :crypto.hash(:sha, :public_key.der_encode(:RSAPublicKey, public_key))
  end

  defp identifier_extensions(identifier),
    do: [subject_identifier(identifier), authority_identifier(identifier)]

  defp subject_identifier(identifier), do: {:Extension, {2, 5, 29, 14}, false, identifier}

  defp authority_identifier(identifier) do
    value = {:AuthorityKeyIdentifier, identifier, :asn1_NOVALUE, :asn1_NOVALUE}
    {:Extension, {2, 5, 29, 35}, false, value}
  end
end

defmodule FerricStore.HTTPTransportEdgeTest do
  use ExUnit.Case, async: false

  alias FerricStore.ClientIdentity
  alias FerricStore.HTTP.{Error, RequestLimit}
  alias FerricStore.SDK
  alias FerricStore.SDK.Native.AdmissionGate
  alias FerricStore.SDK.Native.Client, as: NativeClient

  test "a real HTTP/2 connection executes concurrent SDK commands" do
    reference = make_ref()
    files = FerricStore.HTTPTestTLS.files()

    dispatch =
      :cowboy_router.compile([
        {:_, [{"/v1/commands", FerricStore.HTTP2TestHandler, self()}]}
      ])

    assert {:ok, _listener} =
             :cowboy.start_tls(
               reference,
               %{socket_opts: [port: 0, certfile: files.certfile, keyfile: files.keyfile]},
               %{env: %{dispatch: dispatch}, protocols: [:http2]}
             )

    on_exit(fn ->
      :cowboy.stop_listener(reference)
      File.rm_rf!(files.directory)
    end)

    port = :ranch.get_port(reference)

    assert {:ok, client} =
             SDK.from_url("https://127.0.0.1:#{port}",
               http2: true,
               username: "worker",
               password: "secret"
             )

    assert Enum.all?(
             1..10
             |> Task.async_stream(fn _index -> SDK.ping(client) end, max_concurrency: 10),
             fn {:ok, {:ok, "PONG"}} -> true end
           )

    for _index <- 1..10 do
      assert_receive {:http_version, :"HTTP/2"}
      assert_receive {:http_authorization, "Basic " <> encoded}
      assert Base.decode64!(encoded) == "worker:secret"
    end

    assert :ok = SDK.close(client)
  end

  test "sequential HTTP/1.1 commands reuse their keep-alive connection" do
    url = start_clear_server(FerricStore.HTTPKeepAliveTestHandler, self())
    assert {:ok, client} = SDK.from_url(url)

    assert {:ok, "PONG"} = SDK.ping(client)
    assert_receive {:http_peer, first_peer}
    assert {:ok, "PONG"} = SDK.ping(client)
    assert_receive {:http_peer, second_peer}
    assert second_peer == first_peer
    assert :ok = SDK.close(client)
  end

  test "whole-request timeout covers a trickling response" do
    {:ok, counter} = Agent.start_link(fn -> 0 end)
    reference = make_ref()

    dispatch =
      :cowboy_router.compile([
        {:_, [{"/v1/commands", FerricStore.HTTPSlowTestHandler, counter}]}
      ])

    assert {:ok, listener} =
             :cowboy.start_clear(
               reference,
               %{socket_opts: [port: 0]},
               %{env: %{dispatch: dispatch}, protocols: [:http]}
             )

    Process.unlink(listener)
    on_exit(fn -> :cowboy.stop_listener(reference) end)
    port = :ranch.get_port(reference)
    assert {:ok, warm_client} = SDK.from_url("http://127.0.0.1:#{port}", timeout: 500)
    assert {:ok, "PONG"} = SDK.ping(warm_client)
    assert :ok = SDK.close(warm_client)
    assert {:ok, client} = SDK.from_url("http://127.0.0.1:#{port}", timeout: 50)

    assert {:error, %Error{reason: :timeout, retryable: true, safe_to_retry: false}} =
             SDK.ping(client)

    assert :ok = SDK.close(client)
  end

  test "HTTP execution honors the request deadline instead of its coordinator reply margin" do
    counter = :atomics.new(1, signed: false)
    url = start_clear_server(FerricStore.HTTPDeadlineTestHandler, counter)

    assert {:ok, client} = SDK.from_url(url, timeout: 1_000)

    assert {:ok, "PONG"} = SDK.ping(client)
    assert {:error, %Error{reason: :timeout}} = SDK.ping(client, "PONG", timeout: 30)
    assert :ok = SDK.close(client)
  end

  test "caller death releases synchronous HTTP admission", %{test: test} do
    url = start_admission_server(self(), test)

    assert {:ok, client} =
             SDK.from_url(url, max_concurrent_requests: 1, timeout: 1_000)

    {request, monitor} = spawn_monitor(fn -> SDK.ping(client) end)
    assert_receive {^test, :request_started}
    Process.exit(request, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^request, :killed}

    assert_admission_empty(client)
    assert {:ok, "PONG"} = SDK.ping(client)
    assert :ok = SDK.close(client)
  end

  test "async cancellation releases HTTP admission", %{test: test} do
    url = start_admission_server(self(), test)

    assert {:ok, client} =
             SDK.from_url(url, max_concurrent_requests: 1, timeout: 1_000)

    request = FerricStore.async_native(client, :ping, %{"message" => "PONG"})
    assert_receive {^test, :request_started}
    assert {:error, :client_backpressure} = SDK.ping(client)
    assert :ok = FerricStore.cancel_async(request)

    assert_admission_empty(client)
    assert {:ok, "PONG"} = SDK.ping(client)
    assert :ok = SDK.close(client)
  end

  test "response and batch limits reject bounded work" do
    bypass = Bypass.open()

    Bypass.expect_once(bypass, "POST", "/v1/commands", fn conn ->
      Plug.Conn.send_resp(conn, 200, String.duplicate("x", 128))
    end)

    assert {:ok, client} =
             SDK.from_url("http://127.0.0.1:#{bypass.port}",
               max_batch_items: 1,
               max_response_bytes: 32
             )

    assert {:error, %Error{reason: :response_too_large}} = SDK.ping(client)

    assert {:error, %Error{reason: :batch_too_large}} =
             NativeClient.pipeline(client, [["PING"], ["PING"]], [], [])

    assert :ok = SDK.close(client)

    assert {:ok, request_limited} =
             SDK.from_url("http://127.0.0.1:1", max_request_bytes: 1)

    assert {:error, %Error{reason: :request_too_large}} = SDK.ping(request_limited)
    assert :ok = SDK.close(request_limited)
  end

  test "oversized binary payloads are rejected by the allocation-safe preflight" do
    oversized = :binary.copy(<<255>>, 1_024)

    assert {:error, :request_too_large} = RequestLimit.preflight([["ECHO", oversized]], 128)

    assert :ok = RequestLimit.preflight([["ECHO", "small"]], 128)
  end

  test "server errors retain retry metadata without declaring POST replay safe" do
    bypass = Bypass.open()

    Bypass.expect_once(bypass, "POST", "/v1/commands", fn conn ->
      conn = Plug.Conn.put_resp_header(conn, "retry-after", "2")
      body = Jason.encode!(%{"error" => %{"code" => "busy", "message" => "full"}})
      Plug.Conn.send_resp(conn, 429, body)
    end)

    assert {:ok, client} = SDK.from_url("http://127.0.0.1:#{bypass.port}")

    assert {:error,
            %Error{
              status_code: 429,
              error_code: "busy",
              retry_after_ms: 2_000,
              retryable: true,
              safe_to_retry: false
            }} = SDK.ping(client)

    assert :ok = SDK.close(client)
  end

  test "malformed server error metadata cannot create an invalid exception" do
    bypass = Bypass.open()

    Bypass.expect_once(bypass, "POST", "/v1/commands", fn conn ->
      body = Jason.encode!(%{"error" => %{"code" => 123, "message" => ["invalid"]}})
      Plug.Conn.send_resp(conn, 503, body)
    end)

    assert {:ok, client} = SDK.from_url("http://127.0.0.1:#{bypass.port}")

    assert {:error, %Error{error_code: nil} = error} = SDK.ping(client)
    assert Exception.message(error) == "HTTP command request failed with status 503"
    assert :ok = SDK.close(client)
  end

  test "Basic credentials require HTTPS and session commands fail before network IO" do
    assert {:error, {:invalid_http_credentials, :https_required}} =
             SDK.from_url("http://127.0.0.1:1", username: "worker", password: "secret")

    assert {:ok, client} = SDK.from_url("http://127.0.0.1:1", bearer_token: "token")

    for command <- ["MULTI", "RESET", "MONITOR", "READONLY", "SSUBSCRIBE"] do
      assert {:error, %Error{error_code: "native_only"}} =
               FerricStore.command(client, command)
    end

    assert :ok = SDK.close(client)
  end

  test "single-request blocking commands remain available over HTTP" do
    bypass = Bypass.open()

    Bypass.expect_once(bypass, "POST", "/v1/commands", fn conn ->
      response = %{
        "encoding" => "ferricstore-json-v1",
        "results" => [%{"status" => "ok", "value" => ["jobs", "item"]}]
      }

      Plug.Conn.send_resp(conn, 200, Jason.encode!(response))
    end)

    assert {:ok, client} = SDK.from_url("http://127.0.0.1:#{bypass.port}")
    assert ["jobs", "item"] = FerricStore.command(client, "BLPOP", ["jobs", 1])
    assert :ok = SDK.close(client)
  end

  test "asynchronous commands use the same HTTP transport" do
    bypass = Bypass.open()

    Bypass.expect_once(bypass, "POST", "/v1/commands", fn conn ->
      body = %{
        "encoding" => "ferricstore-json-v1",
        "results" => [%{"status" => "ok", "value" => "PONG"}]
      }

      Plug.Conn.send_resp(conn, 200, Jason.encode!(body))
    end)

    assert {:ok, client} = SDK.from_url("http://127.0.0.1:#{bypass.port}")
    request = FerricStore.async_native(client, :ping, %{"message" => "PONG"})
    assert "PONG" = FerricStore.await(request)
    assert :ok = SDK.close(client)
  end

  defp assert_admission_empty(client, attempts \\ 100)

  defp assert_admission_empty(client, attempts) when attempts > 0 do
    {:ok, endpoint} = ClientIdentity.endpoint(client)
    [{:submission_admission, gate}] = :ets.lookup(endpoint, :submission_admission)

    if AdmissionGate.size(gate) == 0 do
      :ok
    else
      Process.sleep(5)
      assert_admission_empty(client, attempts - 1)
    end
  end

  defp assert_admission_empty(_client, 0), do: flunk("HTTP admission lease was not released")

  defp start_admission_server(owner, tag) do
    counter = :atomics.new(1, signed: false)
    state = %{counter: counter, owner: owner, tag: tag}
    start_clear_server(FerricStore.HTTPAdmissionTestHandler, state)
  end

  defp start_clear_server(handler, handler_state) do
    reference = make_ref()

    dispatch =
      :cowboy_router.compile([
        {:_, [{"/v1/commands", handler, handler_state}]}
      ])

    {:ok, listener} =
      :cowboy.start_clear(
        reference,
        %{socket_opts: [port: 0]},
        %{env: %{dispatch: dispatch}, protocols: [:http]}
      )

    Process.unlink(listener)
    on_exit(fn -> :cowboy.stop_listener(reference) end)
    "http://127.0.0.1:#{:ranch.get_port(reference)}"
  end
end
