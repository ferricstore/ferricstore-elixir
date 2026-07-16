defmodule FerricStore.Transport.EventDispatcherOptions do
  @moduledoc false

  alias FerricStore.{OptionList, Timeout}

  @max_options 4

  @spec parse(term(), pos_integer(), pos_integer()) :: {pos_integer(), pos_integer()}
  def parse(opts, default_max_queue, default_commit_timeout) do
    opts = if OptionList.validate(opts, @max_options) == :ok, do: opts, else: []

    max_queue = positive_integer(Keyword.get(opts, :max_queue), default_max_queue)
    commit_timeout = finite_positive(Keyword.get(opts, :commit_timeout), default_commit_timeout)
    {max_queue, commit_timeout}
  end

  defp positive_integer(value, _default) when is_integer(value) and value > 0, do: value
  defp positive_integer(_value, default), do: default

  defp finite_positive(value, default) do
    if is_integer(value) and Timeout.positive?(value), do: value, else: default
  end
end
