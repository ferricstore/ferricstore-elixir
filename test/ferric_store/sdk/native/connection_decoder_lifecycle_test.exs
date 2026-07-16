defmodule FerricStore.SDK.Native.ConnectionDecoderLifecycleTest do
  use ExUnit.Case, async: true

  alias FerricStore.SDK.Native.{
    Codec,
    ConnectionResponseDecoder,
    ConnectionServerFrameDecoder
  }

  test "an active response decoder terminates with a hard-killed connection owner" do
    response = %{
      target: :heartbeat,
      opcode: 0x0003,
      flags: 0,
      body: <<0::16, Codec.encode_value("PONG")::binary>>,
      max_response_bytes: 1_024,
      response_context: nil
    }

    assert_decoder_follows_owner(fn owner ->
      decode_token = make_ref()
      worker = ConnectionResponseDecoder.start(owner, 1, decode_token, response)

      receive do
        {:ferricstore_response_decoded, ^worker, 1, ^decode_token, {:heartbeat, :ok}} ->
          worker
      end
    end)
  end

  test "an active server-frame decoder terminates with a hard-killed connection owner" do
    frame = %{
      kind: :management,
      opcode: 0x0010,
      flags: 0,
      body: <<0::16, Codec.encode_value(%{"kind" => "event"})::binary>>,
      max_response_bytes: 1_024,
      event_handler: self()
    }

    assert_decoder_follows_owner(fn owner ->
      decode_token = make_ref()
      worker = ConnectionServerFrameDecoder.start(owner, decode_token, frame)

      receive do
        {:ferricstore_server_frame_decoded, ^worker, ^decode_token, :deliver} -> worker
      end
    end)
  end

  test "an authorized response delivery survives an abnormal owner exit" do
    test_pid = self()
    request_id = 17
    decode_token = make_ref()
    response_tag = make_ref()

    response = %{
      target: {:message, test_pid, response_tag},
      opcode: 0x0003,
      flags: 0,
      body: <<0::16, Codec.encode_value("PONG")::binary>>,
      max_response_bytes: 1_024,
      response_context: nil
    }

    owner =
      spawn(fn ->
        worker = ConnectionResponseDecoder.start(self(), request_id, decode_token, response)

        receive do
          {:ferricstore_response_decoded, ^worker, ^request_id, ^decode_token, _metadata} ->
            send(test_pid, {:response_decoder_ready, self(), worker})
        end

        receive do
          :authorize_and_exit ->
            :ok = ConnectionResponseDecoder.deliver(worker, request_id, decode_token)
            Process.exit(self(), :kill)
        end
      end)

    assert_receive {:response_decoder_ready, ^owner, worker}, 1_000

    on_exit(fn ->
      if Process.alive?(worker) do
        _resumed = :erlang.resume_process(worker)
        Process.exit(worker, :kill)
      end

      if Process.alive?(owner), do: Process.exit(owner, :kill)
    end)

    true = :erlang.suspend_process(worker)
    owner_monitor = Process.monitor(owner)
    worker_monitor = Process.monitor(worker)
    send(owner, :authorize_and_exit)

    assert_receive {:DOWN, ^owner_monitor, :process, ^owner, :killed}, 1_000
    assert Process.alive?(worker)

    true = :erlang.resume_process(worker)

    assert_receive {:ferricstore_connection_response, ^owner, ^response_tag, {:ok, "PONG"}},
                   1_000

    assert_receive {:DOWN, ^worker_monitor, :process, ^worker, :normal}, 1_000
  end

  defp assert_decoder_follows_owner(start_decoder) do
    test_pid = self()

    owner =
      spawn(fn ->
        worker = start_decoder.(self())
        send(test_pid, {:decoder_started, self(), worker})
        Process.sleep(:infinity)
      end)

    assert_receive {:decoder_started, ^owner, worker}, 1_000

    on_exit(fn ->
      if Process.alive?(worker) do
        Process.exit(worker, :kill)
      end

      if Process.alive?(owner), do: Process.exit(owner, :kill)
    end)

    monitor = Process.monitor(worker)

    Process.exit(owner, :kill)

    assert_receive {:DOWN, ^monitor, :process, ^worker, :killed}, 1_000
  end
end
