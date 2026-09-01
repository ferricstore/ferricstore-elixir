defmodule FerricStore.Flow.DurableStep do
  @moduledoc false

  alias FerricStore.Codec.Raw

  alias FerricStore.Flow.{
    CodecError,
    CodecRuntime,
    DurableMutationOutcome,
    DurableMutationOutcomeUnknownError,
    DurableStepClaim,
    DurableStepJournal,
    DurableStepOptions,
    ResponseDecodeRuntime
  }

  alias FerricStore.{ClientIdentity, RequestContext, Result, Types}
  alias FerricStore.SDK.Flow, as: NativeFlow

  @spec advance(pid(), map(), keyword()) ::
          map() | {:error, FerricStore.Error.t() | DurableMutationOutcomeUnknownError.t()}
  def advance(client, job, opts) do
    with {:ok, opts} <- DurableStepOptions.prepare(:advance, opts),
         {:ok, job} <- DurableStepClaim.validate(job, :advance),
         :ok <- client_available(client),
         {:ok, claim} <- continue(client, job, opts, %{}) do
      claim
    else
      {:error, %FerricStore.Error{} = error} -> {:error, error}
      {:error, %DurableMutationOutcomeUnknownError{} = error} -> {:error, error}
      {:error, %{__exception__: true} = error} -> {:error, error}
      {:error, reason} -> Result.error(reason)
    end
  end

  @spec step(pid(), map(), keyword()) ::
          {map(), term()}
          | {:error, FerricStore.Error.t() | DurableMutationOutcomeUnknownError.t()}
  def step(client, job, opts) do
    with {:ok, opts} <- DurableStepOptions.prepare(:step, opts),
         {:ok, job} <- DurableStepClaim.validate(job, :step),
         :ok <- client_available(client),
         {:ok, record} <- extend_lease(client, job, opts) do
      run_or_replay(client, job, record, opts)
    else
      {:error, %FerricStore.Error{} = error} -> {:error, error}
      {:error, %DurableMutationOutcomeUnknownError{} = error} -> {:error, error}
      {:error, %{__exception__: true} = error} -> {:error, error}
      {:error, reason} -> Result.error(reason)
    end
  end

  @doc false
  @spec result_name(binary()) :: binary()
  def result_name(name), do: DurableStepJournal.result_name(name)

  defp run_or_replay(client, job, record, opts) do
    name = result_name(Keyword.fetch!(opts, :name))

    case DurableStepJournal.committed_ref(record, name) do
      {:ok, ref} -> replay_committed(client, job, record, ref, opts)
      :missing -> execute_and_continue(client, job, name, opts)
      {:error, reason} -> Result.error(reason)
    end
  end

  defp replay_committed(client, job, record, ref, opts) do
    if Types.get(record, :run_state) == Keyword.fetch!(opts, :to_state) do
      replay(client, DurableStepClaim.refresh(job, record), ref, opts)
    else
      invalid_response(:committed_result_state_mismatch)
    end
  end

  defp execute_and_continue(client, job, name, opts) do
    result = Keyword.fetch!(opts, :run).()

    with {:ok, encoded} <- encode_result(Keyword.get(opts, :codec, Raw), result),
         {:ok, stored_result} <- decode_result(encoded, opts) do
      case continue(client, job, opts, %{name => encoded}) do
        {:ok, refreshed} -> {refreshed, stored_result}
        {:error, %FerricStore.Error{} = error} -> {:error, error}
        {:error, %DurableMutationOutcomeUnknownError{} = error} -> {:error, error}
        {:error, %{__exception__: true} = error} -> {:error, error}
        {:error, reason} -> Result.error(reason)
      end
    else
      {:error, %FerricStore.Error{} = error} -> {:error, error}
      {:error, %{__exception__: true} = error} -> {:error, error}
      {:error, reason} -> Result.error(reason)
    end
  end

  defp replay(client, job, ref, opts) do
    transport_opts = DurableStepOptions.transport_options(opts)

    case NativeFlow.value_mget(client, %{"refs" => [ref]}, transport_opts) do
      {:ok, [encoded]} when is_binary(encoded) -> decode_replay(job, encoded, opts)
      {:ok, [nil]} -> invalid_response(:missing_committed_value)
      {:ok, _other} -> invalid_response(:unexpected_committed_value_response)
      {:error, _reason} = error -> DurableMutationOutcome.normalize(error)
    end
  end

  defp decode_replay(job, encoded, opts) do
    case decode_result(encoded, opts) do
      {:ok, result} -> {job, result}
      {:error, _reason} = error -> error
    end
  end

  defp decode_result(encoded, opts) do
    codec = Keyword.get(opts, :codec, Raw)
    context = RequestContext.new(DurableStepOptions.transport_options(opts), 5_000)

    case ResponseDecodeRuntime.run(context, codec, fn -> codec.decode(encoded) end) do
      {:error, _reason} = error -> error
      result -> {:ok, result}
    end
  end

  defp extend_lease(client, job, opts) do
    payload =
      job
      |> DurableStepClaim.credentials()
      |> put_option(opts, "lease_ms", :lease_ms)
      |> put_option(opts, "now_ms", :now_ms)

    case NativeFlow.extend_lease(client, payload, DurableStepOptions.transport_options(opts)) do
      {:ok, record} -> DurableStepClaim.validate_extended(job, record)
      {:error, _reason} = error -> DurableMutationOutcome.normalize(error)
    end
  end

  defp continue(client, job, opts, values) do
    payload =
      job
      |> DurableStepClaim.credentials()
      |> Map.put("from_state", Types.get(job, :run_state))
      |> Map.put("to_state", Keyword.fetch!(opts, :to_state))
      |> Map.put("return", "JOBS_COMPACT")
      |> put_option(opts, "lease_ms", :lease_ms)
      |> put_option(opts, "now_ms", :now_ms)
      |> put_values(values)

    case NativeFlow.request(
           client,
           :flow_step_continue,
           payload,
           DurableStepOptions.transport_options(opts)
         ) do
      {:ok, claim} ->
        case DurableStepClaim.normalize_refreshed(
               job,
               claim,
               Keyword.fetch!(opts, :to_state)
             ) do
          {:ok, refreshed} -> {:ok, refreshed}
          {:error, reason} -> DurableMutationOutcome.unknown({:error, reason})
        end

      {:error, _reason} = error ->
        DurableMutationOutcome.from_error(error)
    end
  end

  defp encode_result(codec, result) do
    {:ok, CodecRuntime.encode(codec, result)}
  rescue
    _error in CodecError -> {:error, {:flow_codec_encode_failed, codec}}
  end

  defp invalid_response(reason),
    do: Result.error({:invalid_flow_response, %{operation: :step, reason: reason}})

  defp client_available(client) when is_pid(client) do
    case ClientIdentity.type(client) do
      :dead -> {:error, :client_closed}
      :unknown -> {:error, {:client_unavailable, :unknown}}
      _available -> :ok
    end
  end

  defp client_available(_client), do: {:error, {:client_unavailable, :invalid_client}}

  defp put_option(payload, opts, key, option) do
    case Keyword.fetch(opts, option) do
      {:ok, value} -> Map.put(payload, key, value)
      :error -> payload
    end
  end

  defp put_values(payload, values) when map_size(values) == 0, do: payload
  defp put_values(payload, values), do: Map.put(payload, "values", values)
end
