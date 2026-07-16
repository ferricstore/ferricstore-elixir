defmodule FerricStore.Transport.FrameLimits do
  @moduledoc false

  @max_response_chunk_frames 65_536

  @spec max_response_chunk_frames() :: pos_integer()
  def max_response_chunk_frames, do: @max_response_chunk_frames
end
