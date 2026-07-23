defmodule FerricStore.Flow.QueryRequest do
  @moduledoc false

  alias FerricStore.Flow.QueryResponse
  alias FerricStore.Protocol.Opcodes
  alias FerricStore.RequestContext
  alias FerricStore.SDK.Native.PreparedRequests

  @language_version "FQL1"
  @max_query_bytes 16 * 1_024
  @max_parameters 64
  @max_parameter_name_bytes 128
  @min_integer -9_223_372_036_854_775_808
  @max_integer 9_223_372_036_854_775_807

  @spec query(pid(), binary(), map(), keyword()) :: {:ok, term()} | {:error, term()}
  def query(client, query, params \\ %{}, opts \\ []) do
    with {:ok, context} <- PreparedRequests.prepare(opts),
         {:ok, payload} <- payload(query, params, :query),
         do: execute_context(client, payload, context, :result)
  end

  @spec explain(pid(), binary(), map(), keyword()) :: {:ok, term()} | {:error, term()}
  def explain(client, query, params \\ %{}, opts \\ []),
    do: explain_with(client, "EXPLAIN ", query, params, opts)

  @spec explain_analyze(pid(), binary(), map(), keyword()) :: {:ok, term()} | {:error, term()}
  def explain_analyze(client, query, params \\ %{}, opts \\ []),
    do: explain_with(client, "EXPLAIN ANALYZE ", query, params, opts)

  @spec indexes(pid(), binary() | nil, keyword()) :: {:ok, term()} | {:error, term()}
  def indexes(client, index_id \\ nil, opts \\ []) do
    with :ok <- validate_index_id(index_id),
         {:ok, context} <- PreparedRequests.prepare(opts),
         args = if(index_id == nil, do: [], else: [index_id]),
         result <- PreparedRequests.command_exec(client, "FLOW.QUERY.INDEXES", args, context),
         :ok <- RequestContext.ensure_active(context) do
      decode(result, :indexes)
    end
  end

  @doc false
  @spec execute_context(pid(), map(), RequestContext.t(), atom()) ::
          {:ok, term()} | {:error, term()}
  def execute_context(client, payload, %RequestContext{} = context, decoder) do
    result = PreparedRequests.request(client, Opcodes.flow_query(), payload, context)

    with :ok <- RequestContext.ensure_active(context) do
      decode(result, decoder)
    end
  end

  @doc false
  @spec payload(binary(), map(), atom()) :: {:ok, map()} | {:error, term()}
  def payload(query, params, mode \\ :query) do
    with {:ok, query} <- validate_query(query),
         :ok <- validate_mode(query, mode),
         :ok <- validate_parameters(params) do
      {:ok, %{"version" => @language_version, "query" => query, "params" => params}}
    end
  end

  defp explain_with(client, prefix, query, params, opts) do
    with {:ok, context} <- PreparedRequests.prepare(opts),
         {:ok, query} <- validate_query(query),
         :ok <- reject_explain_prefix(query),
         {:ok, payload} <- payload(prefix <> String.trim(query), params, :explain),
         do: execute_context(client, payload, context, :explain)
  end

  defp decode({:ok, value}, :result), do: QueryResponse.result(value)
  defp decode({:ok, value}, :explain), do: QueryResponse.explain(value)
  defp decode({:ok, value}, :indexes), do: QueryResponse.indexes(value)

  defp decode({:error, reason}, _decoder) do
    case QueryResponse.diagnostic(reason) do
      {:ok, diagnostic} -> {:error, diagnostic}
      :error -> {:error, reason}
    end
  end

  defp validate_query(query) when is_binary(query) do
    cond do
      not String.valid?(query) -> {:error, {:invalid_flow_query, :invalid_utf8}}
      String.trim(query) == "" -> {:error, {:invalid_flow_query, :empty_query}}
      byte_size(query) > @max_query_bytes -> {:error, {:invalid_flow_query, :too_large}}
      true -> {:ok, query}
    end
  end

  defp validate_query(_query), do: {:error, {:invalid_flow_query, :expected_binary}}

  defp validate_mode(query, :query) do
    if explain_prefix?(query),
      do: {:error, {:invalid_flow_query, :explain_requires_dedicated_api}},
      else: :ok
  end

  defp validate_mode(_query, :explain), do: :ok

  defp validate_parameters(params) when is_map(params) and map_size(params) <= @max_parameters do
    Enum.reduce_while(params, :ok, fn {name, value}, :ok ->
      case validate_parameter(name, value) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp validate_parameters(params) when is_map(params),
    do: {:error, {:invalid_flow_query_parameters, :too_many}}

  defp validate_parameters(_params),
    do: {:error, {:invalid_flow_query_parameters, :expected_map}}

  defp validate_parameter(name, value)
       when is_binary(name) and byte_size(name) in 1..@max_parameter_name_bytes do
    cond do
      not String.valid?(name) -> {:error, {:invalid_flow_query_parameter, name, :name}}
      parameter_value?(value) -> :ok
      true -> {:error, {:invalid_flow_query_parameter, name, :type}}
    end
  end

  defp validate_parameter(name, _value),
    do: {:error, {:invalid_flow_query_parameter, name, :name}}

  defp parameter_value?(value) when is_binary(value) or is_boolean(value), do: true

  defp parameter_value?(value) when is_integer(value),
    do: value >= @min_integer and value <= @max_integer

  defp parameter_value?(value) when is_float(value) do
    case :erlang.float_to_binary(value, [:compact]) do
      text when text in ["nan", "inf", "-inf"] -> false
      _finite -> true
    end
  rescue
    _error -> false
  end

  defp parameter_value?(_value), do: false

  defp explain_prefix?(query) do
    case Regex.run(~r/^\s*EXPLAIN(?:\s|$)/iu, query) do
      nil -> false
      _match -> true
    end
  end

  defp reject_explain_prefix(query) do
    if explain_prefix?(query),
      do: {:error, {:invalid_flow_query, :already_explain}},
      else: :ok
  end

  defp validate_index_id(nil), do: :ok

  defp validate_index_id(index_id)
       when is_binary(index_id) and byte_size(index_id) in 1..64 do
    if String.valid?(index_id) and Regex.match?(~r/^[A-Za-z0-9_.:-]+$/u, index_id),
      do: :ok,
      else: {:error, {:invalid_flow_query_index_id, index_id}}
  end

  defp validate_index_id(index_id),
    do: {:error, {:invalid_flow_query_index_id, index_id}}
end
