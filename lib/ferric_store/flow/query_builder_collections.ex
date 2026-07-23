defmodule FerricStore.Flow.QueryBuilderCollections do
  @moduledoc false

  alias FerricStore.Flow.Options.PreparedMap
  alias FerricStore.Flow.QueryBuilderCore

  @max_time 9_007_199_254_740_991

  def terminals(opts) do
    type = Keyword.fetch!(opts, :type)

    with :ok <- require_concrete_type(type, :terminals),
         :ok <- reject_attributes(opts),
         {:ok, builder} <- QueryBuilderCore.base(opts),
         {:ok, builder} <- QueryBuilderCore.add_type(builder, type),
         {:ok, builder} <-
           QueryBuilderCore.add_terminal_state(builder, Keyword.get(opts, :state)),
         {:ok, builder} <- QueryBuilderCore.add_window(builder, opts),
         do: QueryBuilderCore.finish(builder)
  end

  def failures(opts) do
    type = Keyword.fetch!(opts, :type)
    attributes = opts |> Keyword.get(:attributes, %{}) |> PreparedMap.unwrap()

    with :ok <- require_failure_source(type, attributes),
         {:ok, builder} <- QueryBuilderCore.base(opts),
         {:ok, builder} <- QueryBuilderCore.add_type(builder, type),
         {:ok, builder} <- QueryBuilderCore.equality(builder, "state", "state", "failed"),
         {:ok, builder} <- QueryBuilderCore.add_attributes(builder, attributes),
         {:ok, builder} <- QueryBuilderCore.add_window(builder, opts),
         do: QueryBuilderCore.finish(builder)
  end

  def lineage(kind, id, opts) when kind in [:parent, :root, :correlation] do
    field =
      case kind do
        :parent -> "parent_flow_id"
        :root -> "root_flow_id"
        :correlation -> "correlation_id"
      end

    with :ok <- reject_attributes(opts),
         {:ok, builder} <- QueryBuilderCore.base(opts),
         {:ok, builder} <- QueryBuilderCore.equality(builder, field, "lineage_id", id),
         {:ok, builder} <-
           QueryBuilderCore.add_optional_state(builder, Keyword.get(opts, :state)),
         {:ok, builder} <- QueryBuilderCore.add_window(builder, opts),
         do: QueryBuilderCore.finish(builder)
  end

  def stuck(opts) do
    type = Keyword.fetch!(opts, :type)
    now_ms = Keyword.get(opts, :now_ms, System.system_time(:millisecond))
    older_than_ms = Keyword.get(opts, :older_than_ms, 0)

    with :ok <- require_concrete_type(type, :stuck),
         :ok <- validate_stuck_time(now_ms, older_than_ms),
         {:ok, builder} <- QueryBuilderCore.base(opts, "lease_deadline_ms"),
         {:ok, builder} <- QueryBuilderCore.add_type(builder, type),
         {:ok, builder} <- QueryBuilderCore.equality(builder, "state", "state", "running"),
         {:ok, builder} <-
           QueryBuilderCore.between(
             builder,
             "lease_deadline_ms",
             "lease_from_ms",
             0,
             "lease_to_ms",
             now_ms - older_than_ms
           ),
         do: QueryBuilderCore.finish(builder)
  end

  defp require_failure_source("any", attributes) when map_size(attributes) == 0,
    do: {:error, {:invalid_flow_query_option, :bounded_source}}

  defp require_failure_source(_type, _attributes), do: :ok

  defp require_concrete_type(type, _operation)
       when is_binary(type) and type != "" and type != "any",
       do: :ok

  defp require_concrete_type(_type, operation),
    do: {:error, {:invalid_flow_query_option, {operation, :concrete_type}}}

  defp reject_attributes(opts) do
    case opts |> Keyword.get(:attributes, %{}) |> PreparedMap.unwrap() do
      attributes when is_map(attributes) and map_size(attributes) == 0 -> :ok
      _attributes -> {:error, {:unsupported_flow_query_option, :attributes}}
    end
  end

  defp validate_stuck_time(now_ms, older_than_ms)
       when is_integer(now_ms) and now_ms >= 0 and now_ms <= @max_time and
              is_integer(older_than_ms) and older_than_ms >= 0 and
              older_than_ms <= now_ms,
       do: :ok

  defp validate_stuck_time(_now_ms, _older_than_ms),
    do: {:error, {:invalid_flow_query_option, :stuck_time}}
end
