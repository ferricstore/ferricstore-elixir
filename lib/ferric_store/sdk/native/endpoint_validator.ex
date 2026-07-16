defmodule FerricStore.SDK.Native.EndpointValidator do
  @moduledoc false

  alias FerricStore.{FailureFormatter, Timeout}

  @spec validate(nil | (map() -> term()), map()) :: :ok | {:error, term()}
  def validate(nil, _endpoint), do: :ok

  def validate(validator, endpoint) when is_function(validator, 1),
    do: validate(validator, endpoint, :infinity)

  def validate(_validator, _endpoint), do: {:error, :invalid_endpoint_validator}

  @spec validate(nil | (map() -> term()), map(), timeout()) :: :ok | {:error, term()}
  def validate(nil, _endpoint, timeout) do
    if Timeout.valid?(timeout), do: :ok, else: invalid_timeout(timeout)
  end

  def validate(validator, endpoint, timeout) when is_function(validator, 1) do
    if Timeout.valid?(timeout) do
      owner = self()
      token = make_ref()
      reply_alias = Process.alias()

      {guardian, monitor} =
        spawn_monitor(fn ->
          validation_guardian(owner, reply_alias, token, validator, endpoint)
        end)

      await_validation(guardian, monitor, reply_alias, token, timeout)
    else
      invalid_timeout(timeout)
    end
  end

  def validate(_validator, _endpoint, timeout) do
    if Timeout.valid?(timeout),
      do: {:error, :invalid_endpoint_validator},
      else: invalid_timeout(timeout)
  end

  defp await_validation(guardian, monitor, reply_alias, token, timeout) do
    receive do
      {^token, result} ->
        Process.unalias(reply_alias)
        Process.demonitor(monitor, [:flush])
        result

      {:DOWN, ^monitor, :process, ^guardian, reason} ->
        Process.unalias(reply_alias)
        {:error, {:endpoint_validator_failed, {:exit, reason}}}
    after
      timeout ->
        Process.unalias(reply_alias)
        send(guardian, {:cancel_validation, owner: self(), token: token})
        Process.demonitor(monitor, [:flush])
        {:error, :timeout}
    end
  end

  defp validation_guardian(owner, reply_alias, token, validator, endpoint) do
    owner_monitor = Process.monitor(owner)
    guardian = self()

    {worker, worker_monitor} =
      spawn_monitor(fn ->
        send(guardian, {:validation_result, self(), validate_inline(validator, endpoint)})
      end)

    await_guarded_validation(
      owner,
      owner_monitor,
      reply_alias,
      token,
      worker,
      worker_monitor
    )
  end

  defp await_guarded_validation(
         owner,
         owner_monitor,
         reply_alias,
         token,
         worker,
         worker_monitor
       ) do
    receive do
      {:validation_result, ^worker, result} ->
        Process.demonitor(worker_monitor, [:flush])
        Process.demonitor(owner_monitor, [:flush])
        send(reply_alias, {token, result})

      {:DOWN, ^worker_monitor, :process, ^worker, reason} ->
        Process.demonitor(owner_monitor, [:flush])

        send(
          reply_alias,
          {token, {:error, {:endpoint_validator_failed, {:exit, reason}}}}
        )

      {:DOWN, ^owner_monitor, :process, ^owner, _reason} ->
        stop_validation_worker(worker, worker_monitor)

      {:cancel_validation, owner: ^owner, token: ^token} ->
        Process.demonitor(owner_monitor, [:flush])
        stop_validation_worker(worker, worker_monitor)
    end
  end

  defp stop_validation_worker(worker, monitor) do
    Process.demonitor(monitor, [:flush])
    Process.exit(worker, :kill)
    :ok
  end

  defp invoke(validator, endpoint) do
    validator.(endpoint)
  rescue
    error ->
      {:callback_failed,
       {:error, FailureFormatter.exception_message(error, "endpoint validator failed")}}
  catch
    kind, reason -> {:callback_failed, {kind, reason}}
  end

  defp validate_inline(validator, endpoint),
    do: validator |> invoke(endpoint) |> normalize_result()

  defp normalize_result(:ok), do: :ok
  defp normalize_result({:ok, _value}), do: :ok
  defp normalize_result(true), do: :ok
  defp normalize_result(false), do: {:error, :unsafe_endpoint}
  defp normalize_result({:error, _reason} = error), do: error

  defp normalize_result({:callback_failed, failure}),
    do: {:error, {:endpoint_validator_failed, failure}}

  defp normalize_result(other), do: {:error, {:invalid_endpoint_validator_result, other}}

  defp invalid_timeout(timeout), do: {:error, {:invalid_endpoint_validation_timeout, timeout}}
end
