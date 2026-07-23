defmodule FerricStore.Flow.QueryBuilderListSearch do
  @moduledoc false

  alias FerricStore.Flow.Options.PreparedMap
  alias FerricStore.Flow.Payload.Policy
  alias FerricStore.Flow.QueryBuilderCore

  def list(opts) do
    attributes = opts |> Keyword.get(:attributes, %{}) |> PreparedMap.unwrap()

    with :ok <- require_list_source(Keyword.fetch!(opts, :type), attributes),
         :ok <- require_list_state_source(opts, attributes),
         {:ok, builder} <- QueryBuilderCore.base(opts),
         {:ok, builder} <- QueryBuilderCore.add_type(builder, Keyword.fetch!(opts, :type)),
         {:ok, builder} <- QueryBuilderCore.add_list_state(builder, Keyword.get(opts, :state)),
         {:ok, builder} <- QueryBuilderCore.add_attributes(builder, attributes),
         {:ok, builder} <- QueryBuilderCore.add_window(builder, opts),
         do: QueryBuilderCore.finish(builder)
  end

  def search(opts) do
    attributes = opts |> Keyword.get(:attributes, %{}) |> PreparedMap.unwrap()
    state_meta = PreparedMap.unwrap(Policy.normalize_search_state_meta(opts) || %{})

    with :ok <- require_search_predicate(attributes, state_meta),
         :ok <- require_state_meta_type(Keyword.fetch!(opts, :type), state_meta),
         {:ok, builder} <- QueryBuilderCore.base(opts),
         {:ok, builder} <-
           QueryBuilderCore.add_optional_type(builder, Keyword.fetch!(opts, :type)),
         {:ok, builder} <- QueryBuilderCore.add_search_state(builder, opts),
         {:ok, builder} <- QueryBuilderCore.add_attributes(builder, attributes),
         {:ok, builder} <- QueryBuilderCore.add_state_meta(builder, state_meta),
         {:ok, builder} <- QueryBuilderCore.add_window(builder, opts),
         do: QueryBuilderCore.finish(builder)
  end

  defp require_search_predicate(attributes, state_meta)
       when map_size(attributes) > 0 or map_size(state_meta) > 0,
       do: :ok

  defp require_search_predicate(_attributes, _state_meta),
    do: {:error, {:invalid_flow_query_option, :missing_metadata_predicate}}

  defp require_list_source("any", attributes) when map_size(attributes) == 0,
    do: {:error, {:invalid_flow_query_option, :bounded_source}}

  defp require_list_source(_type, _attributes), do: :ok

  defp require_list_state_source(opts, attributes) when map_size(attributes) == 0 do
    if Keyword.get(opts, :state) in ["any", :any] and
         not Keyword.get(opts, :terminal_only, false),
       do: {:error, {:invalid_flow_query_option, :bounded_source}},
       else: :ok
  end

  defp require_list_state_source(_opts, _attributes), do: :ok

  defp require_state_meta_type(type, state_meta)
       when type in ["", "any"] and map_size(state_meta) > 0,
       do: {:error, {:invalid_flow_query_option, :state_meta_requires_type}}

  defp require_state_meta_type(_type, _state_meta), do: :ok
end
