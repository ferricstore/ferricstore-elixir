defmodule FerricStore.Transport.ServerFrameAssemblerTest do
  use ExUnit.Case, async: true

  alias FerricStore.Transport.ServerFrameAssembler

  test "owns stream accounting and completes frames in wire order" do
    assembler =
      ServerFrameAssembler.new(
        max_streams: 2,
        max_buffer_bytes: 12,
        max_frame_bytes: 8,
        timeout: :infinity
      )

    assert {:ok, assembler} = ServerFrameAssembler.append(assembler, {1, 2}, 0x20, "one")
    assert {:ok, assembler} = ServerFrameAssembler.append(assembler, {1, 2}, 0x28, "two")
    assert ServerFrameAssembler.stream_count(assembler) == 1
    assert ServerFrameAssembler.buffered_bytes(assembler) == 6

    assert {:ok, 0x08, "onetwo!", assembler} =
             ServerFrameAssembler.complete(assembler, {1, 2}, 0, "!")

    assert ServerFrameAssembler.stream_count(assembler) == 0
    assert ServerFrameAssembler.buffered_bytes(assembler) == 0
  end

  test "bounds stream count, each logical frame, and aggregate buffering" do
    assembler =
      ServerFrameAssembler.new(
        max_streams: 1,
        max_buffer_bytes: 5,
        max_frame_bytes: 4,
        timeout: :infinity
      )

    assert {:ok, assembler} = ServerFrameAssembler.append(assembler, :first, 0x20, "123")

    assert {:error, :too_many_server_chunk_streams} =
             ServerFrameAssembler.append(assembler, :second, 0x20, "1")

    assert {:error, :server_frame_too_large} =
             ServerFrameAssembler.append(assembler, :first, 0x20, "12")

    assembler =
      ServerFrameAssembler.new(
        max_streams: 2,
        max_buffer_bytes: 3,
        max_frame_bytes: 8,
        timeout: :infinity
      )

    assert {:ok, assembler} = ServerFrameAssembler.append(assembler, :first, 0x20, "123")

    assert {:error, :server_chunks_too_large} =
             ServerFrameAssembler.append(assembler, :first, 0x20, "4")
  end

  test "counts final and unchunked frames against other buffered streams" do
    assembler =
      ServerFrameAssembler.new(
        max_streams: 2,
        max_buffer_bytes: 10,
        max_frame_bytes: 10,
        timeout: :infinity
      )

    assert {:ok, assembler} = ServerFrameAssembler.append(assembler, :first, 0x20, "123456")
    assert {:ok, assembler} = ServerFrameAssembler.append(assembler, :second, 0x20, "12")

    assert {:error, :server_chunks_too_large, assembler} =
             ServerFrameAssembler.complete(assembler, :second, 0, "3456")

    assert ServerFrameAssembler.stream_count(assembler) == 1
    assert ServerFrameAssembler.buffered_bytes(assembler) == 6

    assert {:error, :server_chunks_too_large, assembler} =
             ServerFrameAssembler.complete(assembler, :unchunked, 0, "12345")

    assert ServerFrameAssembler.stream_count(assembler) == 1
    assert ServerFrameAssembler.buffered_bytes(assembler) == 6
  end

  test "bounds zero-byte chunks by aggregate retained frame count" do
    assembler =
      ServerFrameAssembler.new(
        max_streams: 2,
        max_buffer_bytes: 10,
        max_buffer_frames: 2,
        max_frame_bytes: 10,
        timeout: :infinity
      )

    assert {:ok, assembler} = ServerFrameAssembler.append(assembler, :first, 0x20, "")
    assert {:ok, assembler} = ServerFrameAssembler.append(assembler, :first, 0x20, "")

    assert {:error, :too_many_server_chunk_frames} =
             ServerFrameAssembler.append(assembler, :first, 0x20, "")

    assert {:error, :too_many_server_chunk_frames, assembler} =
             ServerFrameAssembler.complete(assembler, :first, 0, "")

    assert ServerFrameAssembler.stream_count(assembler) == 0
    assert ServerFrameAssembler.buffered_frames(assembler) == 0
  end

  test "retained server chunks do not pin an unrelated receive packet" do
    assembler =
      ServerFrameAssembler.new(
        max_streams: 2,
        max_buffer_bytes: 1_024,
        max_frame_bytes: 1_024,
        timeout: :infinity
      )

    body = retained_slice(65, 4 * 1_024 * 1_024)
    assert :binary.referenced_byte_size(body) > byte_size(body) * 2

    assert {:ok, assembler} = ServerFrameAssembler.append(assembler, :event, 0x20, body)

    retained = assembler.streams.event.chunks |> List.first()
    assert :binary.referenced_byte_size(retained) <= byte_size(retained) * 2
  end

  defp retained_slice(size, unrelated_size) do
    packet = :binary.copy("x", size) <> :binary.copy("y", unrelated_size)
    binary_part(packet, 0, size)
  end
end
