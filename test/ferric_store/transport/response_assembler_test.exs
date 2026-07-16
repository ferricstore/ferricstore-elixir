defmodule FerricStore.Transport.ResponseAssemblerTest do
  use ExUnit.Case, async: true

  alias FerricStore.Transport.ResponseAssembler

  test "accounts for partial chunks and reassembles them in wire order" do
    pending = %{chunks: [], chunk_bytes: 0, chunk_frames: 0, flags: 0}

    assert {:ok, pending, 3, 1} =
             ResponseAssembler.append(pending, 0x20, "one", 0, 0,
               max_response_bytes: 8,
               max_buffer_bytes: 12,
               max_buffer_frames: 4
             )

    assert {:ok, pending, 6, 2} =
             ResponseAssembler.append(pending, 0x28, "two", 3, 1,
               max_response_bytes: 8,
               max_buffer_bytes: 12,
               max_buffer_frames: 4
             )

    assert {:ok, 0x08, "onetwo!"} =
             ResponseAssembler.complete(pending, 0, "!", 6, 2,
               max_response_bytes: 8,
               max_buffer_bytes: 12,
               max_buffer_frames: 4
             )
  end

  test "rejects per-response and aggregate buffer overflow before retaining a chunk" do
    pending = %{chunks: [], chunk_bytes: 5, chunk_frames: 0, flags: 0}

    assert {:error, :response_too_large} =
             ResponseAssembler.append(pending, 0x20, "more", 5, 0,
               max_response_bytes: 8,
               max_buffer_bytes: 20,
               max_buffer_frames: 4
             )

    assert {:error, :response_buffers_too_large} =
             ResponseAssembler.append(%{pending | chunk_bytes: 0}, 0x20, "more", 8, 0,
               max_response_bytes: 20,
               max_buffer_bytes: 10,
               max_buffer_frames: 4
             )
  end

  test "counts a final chunk against aggregate buffered responses" do
    pending = %{chunks: ["12"], chunk_bytes: 2, chunk_frames: 1, flags: 0x20}

    assert {:error, :response_buffers_too_large} =
             ResponseAssembler.complete(pending, 0, "3456", 8, 1,
               max_response_bytes: 8,
               max_buffer_bytes: 10,
               max_buffer_frames: 4
             )
  end

  test "bounds zero-byte response chunks by retained frame count" do
    pending = %{chunks: ["", ""], chunk_bytes: 0, chunk_frames: 2, flags: 0x20}

    assert {:error, :response_chunk_frames_too_large} =
             ResponseAssembler.append(pending, 0x20, "", 0, 2,
               max_response_bytes: 8,
               max_buffer_bytes: 8,
               max_buffer_frames: 2
             )

    assert {:error, :response_chunk_frames_too_large} =
             ResponseAssembler.complete(pending, 0, "", 0, 2,
               max_response_bytes: 8,
               max_buffer_bytes: 8,
               max_buffer_frames: 2
             )
  end

  test "retained response chunks do not pin an unrelated receive packet" do
    body = retained_slice(65, 4 * 1_024 * 1_024)
    pending = %{chunks: [], chunk_bytes: 0, chunk_frames: 0, flags: 0}

    assert :binary.referenced_byte_size(body) > byte_size(body) * 2

    assert {:ok, pending, 65, 1} =
             ResponseAssembler.append(pending, 0x20, body, 0, 0,
               max_response_bytes: 1_024,
               max_buffer_bytes: 1_024,
               max_buffer_frames: 4
             )

    [retained] = pending.chunks
    assert :binary.referenced_byte_size(retained) <= byte_size(retained) * 2
  end

  defp retained_slice(size, unrelated_size) do
    packet = :binary.copy("x", size) <> :binary.copy("y", unrelated_size)
    binary_part(packet, 0, size)
  end
end
