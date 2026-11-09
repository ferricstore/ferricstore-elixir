defmodule FerricStore.Flow.ChildGroupResults do
  @moduledoc """
  A parent's child group, with children ordered by id.

  `wait` is `"all"`, `"any"`, or `"none"`. `resolved` is `"success"` or
  `"failure"` once the group resolved, and `nil` while it waits.
  """

  defstruct [:group_id, :wait, :resolved, summary: %{}, children: []]

  @type t :: %__MODULE__{
          group_id: binary(),
          wait: binary(),
          resolved: binary() | nil,
          summary: %{
            total: non_neg_integer(),
            completed: non_neg_integer(),
            failed: non_neg_integer(),
            cancelled: non_neg_integer()
          },
          children: [FerricStore.Flow.ChildResult.t()]
        }
end
