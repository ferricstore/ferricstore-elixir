defmodule FerricStore.Flow.Children do
  @moduledoc """
  Fan-out and fan-in for claimed Flows.

  `wait_for_children/4` creates child Flows and parks the claimed parent in
  `wait_state` in one fenced `FLOW.SPAWN_CHILDREN`. The parent waits without a
  lease. FerricStore moves it to `success` once the group resolves, or to
  `failure` at the first failed child unless `on_child_failed: :ignore`. Claim
  the success state as usual and read the children with `child_results/3`.

  Fan-in needs FerricStore 0.11.19 or newer.
  """

  alias FerricStore.Codec.Raw
  alias FerricStore.Flow
  alias FerricStore.Flow.{ChildGroupReader, ChildGroupResults, ClaimNormalizer}
  alias FerricStore.{Result, SDK, Types}

  @transport [:timeout]

  @doc """
  Creates `children` and parks the claimed `job` until they finish.

  Each child is a map or keyword list with `:id`, `:type`, and optional
  `:payload` (encoded with `:codec`) and `:partition_key`. Options:
  `:wait_state`, `:success`, and `:failure` (required), `:group_id`
  (`"default"`), `:wait` (`:all` or `:any`), `:on_child_failed`
  (`:fail_parent` or `:ignore`), `:on_parent_closed` (`:cancel_children` or
  `:abandon_children`), `:max_active_ms`, `:codec`, `:now_ms`, and `:timeout`.
  """
  @spec wait_for_children(pid(), map(), [map() | keyword()], keyword()) ::
          term() | {:error, FerricStore.Error.t()}
  def wait_for_children(client, job, children, opts) do
    with {:ok, payload} <- spawn_payload(job, children, opts),
         {:ok, value} <-
           SDK.Flow.spawn_children(client, payload, Keyword.take(opts, @transport)) do
      value
    else
      {:error, reason} -> Result.error(reason)
    end
  end

  @doc """
  Reads one of a parent's child groups: each child's status and its decoded
  result or error, ordered by child id. Returns `nil` when the parent or the
  group does not exist.

  Options: `:group_id` (`"default"`), `:partition_key`, `:codec`, `:max_bytes`,
  and `:timeout`.
  """
  @spec child_results(pid(), binary(), keyword()) ::
          ChildGroupResults.t() | nil | {:error, FerricStore.Error.t()}
  def child_results(client, parent_id, opts \\ []) do
    get_opts = Keyword.take(opts, [:partition_key | @transport])
    mget_opts = Keyword.take(opts, [:codec, :max_bytes | @transport])

    with record when not is_tuple(record) <- Flow.get(client, parent_id, get_opts),
         {:ok, group, refs} when not is_nil(group) <-
           ChildGroupReader.read(record, Keyword.get(opts, :group_id, "default")),
         values when is_list(values) <- Flow.value_mget(client, refs, mget_opts) do
      ChildGroupReader.fill_values(group, values)
    else
      {:ok, nil, []} -> nil
      {:error, %FerricStore.Error{}} = error -> error
      {:error, reason} -> Result.error(reason)
    end
  end

  @doc false
  def spawn_payload(job, children, opts) do
    with {:ok, claim} <- claim(job),
         {:ok, states} <- required_states(opts),
         {:ok, wait} <- wait_mode(Keyword.get(opts, :wait, :all)),
         {:ok, encoded} <- encode_children(children, Keyword.get(opts, :codec, Raw)) do
      payload =
        claim
        |> Map.merge(states)
        |> Map.merge(%{
          "group_id" => Keyword.get(opts, :group_id, "default"),
          "wait" => wait,
          "children" => encoded
        })
        |> put_option("on_child_failed", Keyword.get(opts, :on_child_failed))
        |> put_option("on_parent_closed", Keyword.get(opts, :on_parent_closed))
        |> put_raw("max_active_ms", Keyword.get(opts, :max_active_ms))
        |> put_option("now_ms", Keyword.get(opts, :now_ms))

      {:ok, payload}
    end
  end

  defp claim(job) do
    with {:ok, job} <- ClaimNormalizer.normalize(job),
         id when is_binary(id) and id != "" <- Types.get(job, :id),
         lease when is_binary(lease) and lease != "" <- Types.get(job, :lease_token),
         fence when is_integer(fence) and fence > 0 <- Types.get(job, :fencing_token) do
      {:ok,
       %{
         "id" => id,
         "partition_key" => Types.get(job, :partition_key),
         "lease_token" => lease,
         "fencing_token" => fence
       }}
    else
      _invalid -> {:error, {:invalid_flow_claim, :wait_for_children, :expected_claimed_job}}
    end
  end

  defp required_states(opts) do
    Enum.reduce_while([:wait_state, :success, :failure], {:ok, %{}}, fn key, {:ok, acc} ->
      case Keyword.get(opts, key) do
        state when is_binary(state) and state != "" ->
          {:cont, {:ok, Map.put(acc, Atom.to_string(key), state)}}

        _missing ->
          {:halt, {:error, {:missing_child_wait_option, key}}}
      end
    end)
  end

  defp wait_mode(wait) when wait in [:all, "all"], do: {:ok, "all"}
  defp wait_mode(wait) when wait in [:any, "any"], do: {:ok, "any"}
  defp wait_mode(wait), do: {:error, {:invalid_child_wait, wait}}

  defp encode_children([], _codec), do: {:error, {:invalid_children, :empty}}

  defp encode_children(children, codec) when is_list(children) do
    children
    |> Enum.reduce_while({:ok, []}, fn child, {:ok, acc} ->
      case encode_child(child, codec) do
        {:ok, encoded} -> {:cont, {:ok, [encoded | acc]}}
        :error -> {:halt, {:error, {:invalid_child, child}}}
      end
    end)
    |> case do
      {:ok, encoded} -> {:ok, Enum.reverse(encoded)}
      error -> error
    end
  end

  defp encode_children(children, _codec), do: {:error, {:invalid_children, children}}

  defp encode_child(child, codec) when is_list(child) or is_map(child) do
    child = Map.new(child)

    with id when is_binary(id) and id != "" <- Map.get(child, :id),
         type when is_binary(type) and type != "" <- Map.get(child, :type) do
      encoded =
        %{"id" => id, "type" => type}
        |> put_option("payload", encode_payload(codec, Map.get(child, :payload)))
        |> put_option("partition_key", Map.get(child, :partition_key))

      {:ok, encoded}
    else
      _invalid -> :error
    end
  end

  defp encode_child(_child, _codec), do: :error

  defp encode_payload(_codec, nil), do: nil
  defp encode_payload(codec, payload), do: codec.encode(payload)

  defp put_raw(map, _key, nil), do: map
  defp put_raw(map, key, value), do: Map.put(map, key, value)

  defp put_option(map, _key, nil), do: map

  defp put_option(map, key, value) when is_atom(value),
    do: Map.put(map, key, Atom.to_string(value))

  defp put_option(map, key, value), do: Map.put(map, key, value)
end
