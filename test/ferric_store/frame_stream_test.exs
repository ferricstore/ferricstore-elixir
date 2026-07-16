defmodule FerricStore.FrameStreamTest do
  use ExUnit.Case, async: true

  alias FerricStore.Protocol
  alias FerricStore.Transport.FrameStream

  test "parses a response frame split across arbitrary packet boundaries" do
    body = <<0::unsigned-16, Protocol.encode_value("value")::binary>>
    frame = response_frame(0x0101, 7, 42, 0, body)

    stream =
      frame
      |> :binary.bin_to_list()
      |> Enum.reduce(FrameStream.new(), fn byte, acc ->
        FrameStream.append(acc, <<byte>>)
      end)

    assert FrameStream.byte_size(stream) == byte_size(frame)
    assert {:ok, header, ^body, rest} = FrameStream.next(stream, 1_024)
    assert header.request_id == 42
    assert header.opcode == 0x0101
    assert header.lane_id == 7
    assert FrameStream.empty?(rest)
  end

  test "leaves incomplete data queued and rejects oversized headers before their bodies arrive" do
    frame = response_frame(0x0101, 1, 1, 0, "short")
    <<partial::binary-size(12), _rest::binary>> = frame
    stream = FrameStream.append(FrameStream.new(), partial)

    assert :incomplete = FrameStream.next(stream, 1_024)
    assert FrameStream.byte_size(stream) == 12

    oversized = response_frame(0x0101, 1, 1, 0, String.duplicate("x", 65))
    <<header::binary-size(24), _body::binary>> = oversized

    assert {:error, :frame_too_large} =
             FrameStream.next(FrameStream.append(FrameStream.new(), header), 64)
  end

  test "chunk appends scale linearly" do
    small = measured_reductions(fn -> append_chunks(2_000) end)
    large = measured_reductions(fn -> append_chunks(4_000) end)

    assert large < small * 3
  end

  test "extreme packet fragmentation keeps chunk metadata bounded" do
    stream =
      Enum.reduce(1..100_000, FrameStream.new(), fn _, acc ->
        FrameStream.append(acc, "x")
      end)

    assert FrameStream.byte_size(stream) == 100_000
    assert :queue.len(stream.chunks) < 2_000
  end

  test "a tiny trailing fragment does not retain a large parsed packet" do
    body = :binary.copy("x", 4 * 1_024 * 1_024)
    frame = response_frame(0x0101, 1, 1, 0, body)
    stream = FrameStream.append(FrameStream.new(), frame <> <<1>>)

    assert {:ok, _header, ^body, rest} = FrameStream.next(stream, byte_size(body))
    assert {{:value, trailing_fragment}, _queue} = :queue.out(rest.chunks)
    assert byte_size(trailing_fragment) == 1
    assert :binary.referenced_byte_size(trailing_fragment) <= 64
  end

  test "randomized multi-frame fragmentation preserves every frame in order" do
    :rand.seed(:exsss, {701, 702, 703})

    expected =
      Enum.map(1..1_000, fn request_id ->
        body = :crypto.strong_rand_bytes(:rand.uniform(256) - 1)

        %{
          opcode: :rand.uniform(0xFFFF) - 1,
          lane_id: :rand.uniform(16) - 1,
          request_id: request_id,
          flags: :rand.uniform(4) - 1,
          body: body
        }
      end)

    chunks =
      expected
      |> Enum.map_join(&response_frame(&1.opcode, &1.lane_id, &1.request_id, &1.flags, &1.body))
      |> random_chunks([])

    {stream, remaining} =
      Enum.reduce(chunks, {FrameStream.new(), expected}, fn chunk, {stream, remaining} ->
        stream
        |> FrameStream.append(chunk)
        |> drain_frames(remaining)
      end)

    assert remaining == []
    assert FrameStream.empty?(stream)
  end

  defp append_chunks(count) do
    stream =
      Enum.reduce(1..count, FrameStream.new(), fn _, acc -> FrameStream.append(acc, "x") end)

    assert FrameStream.byte_size(stream) == count
  end

  defp measured_reductions(fun) do
    {:reductions, before_count} = Process.info(self(), :reductions)
    fun.()
    {:reductions, after_count} = Process.info(self(), :reductions)
    after_count - before_count
  end

  defp random_chunks("", chunks), do: Enum.reverse(chunks)

  defp random_chunks(binary, chunks) do
    size = min(byte_size(binary), :rand.uniform(97))
    <<chunk::binary-size(^size), rest::binary>> = binary
    random_chunks(rest, [chunk | chunks])
  end

  defp drain_frames(stream, []), do: {stream, []}

  defp drain_frames(stream, [expected | remaining] = all) do
    case FrameStream.next(stream, 1_024) do
      :incomplete ->
        {stream, all}

      {:ok, header, body, stream} ->
        assert header.opcode == expected.opcode
        assert header.lane_id == expected.lane_id
        assert header.request_id == expected.request_id
        assert header.flags == expected.flags
        assert body == expected.body
        drain_frames(stream, remaining)
    end
  end

  defp response_frame(opcode, lane_id, request_id, flags, body) do
    <<"FSNP", 0x81, flags, lane_id::unsigned-32, opcode::unsigned-16, request_id::unsigned-64,
      byte_size(body)::unsigned-32, body::binary>>
  end
end
