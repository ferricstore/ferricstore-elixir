defmodule FerricStore.Flow.QueryBuilderCore do
  @moduledoc false

  alias FerricStore.Flow.Options.PreparedMap
  alias FerricStore.Flow.{QueryBuilderMetadata, QueryBuilderWindow}

  @max_partition_bytes 65_535
  @max_results 100

  def base(opts, order_field \\ "updated_at_ms") do
    partition = Keyword.get(opts, :partition_key)
    limit = Keyword.get(opts, :count, @max_results)
    reverse = Keyword.get(opts, :rev, false)

    with :ok <- valid_partition(partition),
         :ok <- valid_limit(limit),
         :ok <- valid_reverse(reverse),
         :ok <- disabled_option(opts, :include_cold),
         :ok <- disabled_option(opts, :consistent_projection) do
      {:ok,
       %{
         predicates: ["partition_key = @partition_key"],
         params: %{"partition_key" => partition},
         order_field: order_field,
         direction: if(reverse, do: "DESC", else: "ASC"),
         limit: limit
       }}
    end
  end

  def add_type(builder, "any"), do: {:ok, builder}
  def add_type(builder, type), do: equality(builder, "type", "type", type)
  def add_optional_type(builder, type) when type in [nil, "", "any"], do: {:ok, builder}
  def add_optional_type(builder, type), do: equality(builder, "type", "type", type)

  def add_list_state(builder, nil), do: equality(builder, "state", "state", "queued")
  def add_list_state(builder, state) when state in ["any", :any], do: {:ok, builder}
  def add_list_state(builder, state), do: equality(builder, "state", "state", state)

  def add_search_state(builder, opts) do
    case {Keyword.get(opts, :terminal_only, false), Keyword.get(opts, :state)} do
      {true, state} when state in [nil, "", "any", :any] ->
        add_terminal_state(builder, state)

      {true, state} when state in ["completed", "failed", "cancelled"] ->
        equality(builder, "state", "state", state)

      {true, _state} ->
        {:error, {:invalid_flow_query_option, :state}}

      {false, state} when state in [nil, "", "any", :any] ->
        {:ok, builder}

      {false, state} ->
        equality(builder, "state", "state", state)
    end
  end

  def add_terminal_state(builder, state) when state in [nil, "", "any", :any] do
    {:ok,
     builder
     |> predicate("state IN (@terminal_0, @terminal_1, @terminal_2)")
     |> parameter("terminal_0", "completed")
     |> parameter("terminal_1", "failed")
     |> parameter("terminal_2", "cancelled")}
  end

  def add_terminal_state(builder, state) when state in ["completed", "failed", "cancelled"],
    do: equality(builder, "state", "state", state)

  def add_terminal_state(_builder, _state),
    do: {:error, {:invalid_flow_query_option, :state}}

  def add_optional_state(builder, state) when state in [nil, "", "any", :any],
    do: {:ok, builder}

  def add_optional_state(builder, state), do: equality(builder, "state", "state", state)

  def add_attributes(builder, attributes) when is_map(attributes) do
    attributes = PreparedMap.unwrap(attributes)

    case QueryBuilderMetadata.attributes(attributes) do
      {:ok, entries} ->
        entries
        |> Enum.with_index()
        |> Enum.reduce_while({:ok, builder}, &add_attribute_entry/2)

      :error ->
        {:error, {:invalid_flow_query_option, :attributes}}
    end
  end

  def add_attributes(_builder, _attributes),
    do: {:error, {:invalid_flow_query_option, :attributes}}

  def add_state_meta(builder, state_meta) when is_map(state_meta) do
    state_meta = PreparedMap.unwrap(state_meta)

    case QueryBuilderMetadata.state_meta(state_meta) do
      {:ok, entries} ->
        entries
        |> Enum.with_index()
        |> Enum.reduce_while({:ok, builder}, &add_state_meta_entry/2)

      :error ->
        {:error, {:invalid_flow_query_option, :state_meta}}
    end
  end

  def add_state_meta(_builder, _state_meta),
    do: {:error, {:invalid_flow_query_option, :state_meta}}

  def add_window(builder, opts), do: QueryBuilderWindow.add(builder, opts)

  def equality(builder, field, parameter_name, value) do
    if parameter_value?(value) do
      {:ok,
       builder
       |> predicate("#{field} = @#{parameter_name}")
       |> parameter(parameter_name, value)}
    else
      {:error, {:invalid_flow_query_parameter, parameter_name, :type}}
    end
  end

  def between(builder, field, from_name, from_value, to_name, to_value) do
    {:ok,
     builder
     |> predicate("#{field} BETWEEN @#{from_name} AND @#{to_name}")
     |> parameter(from_name, from_value)
     |> parameter(to_name, to_value)}
  end

  def finish(%{predicates: predicates} = builder) when length(predicates) <= 12 do
    query =
      "FROM runs WHERE " <>
        Enum.join(Enum.reverse(predicates), " AND ") <>
        " ORDER BY #{builder.order_field} #{builder.direction} LIMIT #{builder.limit} RETURN RECORDS"

    {:ok, query, builder.params}
  end

  def finish(_builder), do: {:error, {:invalid_flow_query, :too_many_predicates}}

  defp valid_partition(value)
       when is_binary(value) and value != "" and byte_size(value) <= @max_partition_bytes,
       do: :ok

  defp valid_partition(_value), do: {:error, {:invalid_flow_query_option, :partition_key}}
  defp valid_limit(value) when is_integer(value) and value in 1..@max_results, do: :ok
  defp valid_limit(_value), do: {:error, {:invalid_flow_query_option, :count}}
  defp valid_reverse(value) when value in [nil, false, true], do: :ok
  defp valid_reverse(_value), do: {:error, {:invalid_flow_query_option, :rev}}

  defp disabled_option(opts, option) do
    if Keyword.get(opts, option, false),
      do: {:error, {:unsupported_flow_query_option, option}},
      else: :ok
  end

  defp add_attribute_entry({{name, value}, index}, {:ok, builder}) do
    builder
    |> equality(selector("attribute", [name]), "attribute_#{index}", value)
    |> reduce_result()
  end

  defp add_state_meta_entry({{state, name, value}, index}, {:ok, builder}) do
    builder
    |> equality(selector("state_meta", [state, name]), "state_meta_#{index}", value)
    |> reduce_result()
  end

  defp reduce_result({:ok, next}), do: {:cont, {:ok, next}}
  defp reduce_result({:error, _reason} = error), do: {:halt, error}

  defp predicate(builder, value), do: %{builder | predicates: [value | builder.predicates]}

  defp parameter(builder, name, value),
    do: %{builder | params: Map.put(builder.params, name, value)}

  defp selector(root, names) do
    Enum.reduce(names, root, fn name, acc ->
      acc <> "['" <> String.replace(name, "'", "''") <> "']"
    end)
  end

  defp parameter_value?(value),
    do: is_binary(value) or is_boolean(value) or is_integer(value) or is_float(value)
end
