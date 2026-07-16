defmodule FerricStore.SDK.Native.ClientSeedOptions do
  @moduledoc false

  alias FerricStore.RequestLimits
  alias FerricStore.SDK.Native.EndpointPolicy

  @default_candidate_limit 32
  @max_candidate_limit RequestLimits.max_batch_items()

  @spec validate(keyword()) :: :ok | {:error, {atom(), term()}}
  def validate(opts) when is_list(opts) do
    case candidate_limit(opts) do
      {:ok, limit} -> validate_seeds_option(opts, limit)
      {:error, _reason} = error -> error
    end
  end

  @spec default_candidate_limit() :: pos_integer()
  def default_candidate_limit, do: @default_candidate_limit

  defp validate_seeds_option(opts, limit) do
    case Keyword.fetch(opts, :seeds) do
      {:ok, seeds} ->
        if valid_seeds?(seeds, limit), do: :ok, else: {:error, {:seeds, seeds}}

      :error ->
        {:error, {:seeds, nil}}
    end
  end

  defp candidate_limit(opts) do
    case Keyword.get(opts, :max_refresh_candidates, @default_candidate_limit) do
      value when is_integer(value) and value > 0 and value <= @max_candidate_limit ->
        {:ok, value}

      value ->
        {:error, {:max_refresh_candidates, value}}
    end
  end

  defp valid_seeds?([seed | seeds], limit) when limit > 0,
    do: valid_seed?(seed) and valid_seed_tail?(seeds, limit - 1)

  defp valid_seeds?(_empty_or_invalid, _limit), do: false

  defp valid_seed_tail?([], _remaining), do: true
  defp valid_seed_tail?([_seed | _seeds], 0), do: false

  defp valid_seed_tail?([seed | seeds], remaining),
    do: valid_seed?(seed) and valid_seed_tail?(seeds, remaining - 1)

  defp valid_seed_tail?(_improper, _remaining), do: false

  defp valid_seed?(seed), do: match?({:ok, _endpoint}, EndpointPolicy.normalize(seed))
end
