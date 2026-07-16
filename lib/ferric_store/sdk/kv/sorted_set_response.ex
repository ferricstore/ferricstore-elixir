defmodule FerricStore.SDK.KV.SortedSetResponse do
  @moduledoc false

  alias FerricStore.{DeadlineBudget, RequestLimits}
  alias FerricStore.SDK.KV.{ResponseValue, ScoreResponseParser}

  @max_items RequestLimits.max_batch_items()
  @deadline_check_interval 256

  @spec zrange(term(), boolean()) :: {:ok, list()} | {:error, atom()}
  def zrange(value, false), do: ResponseValue.binary_list(value)
  def zrange(value, true), do: scored_members(value, [], nil, 0)

  def zrange(value, false, %DeadlineBudget{} = budget),
    do: ResponseValue.binary_list(value, budget)

  def zrange(value, true, %DeadlineBudget{} = budget),
    do: scored_members(value, [], budget, 0)

  @spec score(term()) :: {:ok, float() | nil} | {:error, atom()}
  def score(nil), do: {:ok, nil}

  def score(value) when is_binary(value) do
    ScoreResponseParser.parse(value, :expected_score_string_or_nil)
  end

  def score(_value), do: {:error, :expected_score_string_or_nil}

  defp scored_members([], members, budget, _count) do
    with :ok <- ensure_active(budget), do: {:ok, Enum.reverse(members)}
  end

  defp scored_members([_member, _score | _values], _members, budget, @max_items) do
    with :ok <- ensure_active(budget), do: {:error, :too_many_items}
  end

  defp scored_members(values, members, budget, count)
       when rem(count, @deadline_check_interval) == 0 do
    with :ok <- ensure_active(budget),
         do: scored_member(values, members, budget, count)
  end

  defp scored_members(values, members, budget, count),
    do: scored_member(values, members, budget, count)

  defp scored_member([member, score | values], members, budget, count)
       when is_binary(member) and is_binary(score) do
    case ScoreResponseParser.parse(score, :expected_member_score_list) do
      {:ok, parsed} -> scored_members(values, [{member, parsed} | members], budget, count + 1)
      {:error, _reason} = error -> error
    end
  end

  defp scored_member(_value, _members, _budget, _count),
    do: {:error, :expected_member_score_list}

  defp ensure_active(nil), do: :ok
  defp ensure_active(%DeadlineBudget{} = budget), do: DeadlineBudget.ensure_active(budget)
end
