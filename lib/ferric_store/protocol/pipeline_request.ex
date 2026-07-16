defmodule FerricStore.Protocol.PipelineRequest do
  @moduledoc false

  @enforce_keys [:commands, :command_count]
  defstruct [:commands, :command_count, options: []]

  @type t :: %__MODULE__{
          commands: list(),
          command_count: non_neg_integer(),
          options: keyword()
        }
end
