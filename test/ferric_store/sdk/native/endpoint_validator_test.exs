defmodule FerricStore.SDK.Native.EndpointValidatorTest do
  use ExUnit.Case, async: false

  alias FerricStore.SDK.Native.EndpointValidator

  test "finite validation work is cancelled when its owner dies" do
    test_pid = self()

    validator = fn _endpoint ->
      send(test_pid, {:validator_started, self()})

      receive do
        :release_validator -> :ok
      end
    end

    owner =
      spawn(fn ->
        EndpointValidator.validate(validator, %{host: "cache.internal"}, 5_000)
      end)

    assert_receive {:validator_started, validator_worker}, 250
    refute validator_worker == owner

    on_exit(fn ->
      if Process.alive?(owner), do: Process.exit(owner, :kill)
      if Process.alive?(validator_worker), do: Process.exit(validator_worker, :kill)
    end)

    monitor = Process.monitor(validator_worker)
    Process.exit(owner, :kill)

    assert_receive {:DOWN, ^monitor, :process, ^validator_worker, _reason}, 250
  end

  test "infinite validation contains an untrappable callback exit" do
    owner = self()

    {caller, monitor} =
      spawn_monitor(fn ->
        result =
          EndpointValidator.validate(
            fn _endpoint -> Process.exit(self(), :kill) end,
            %{host: "cache.internal"},
            :infinity
          )

        send(owner, {:validation_result, result})
      end)

    assert_receive {:validation_result, {:error, {:endpoint_validator_failed, {:exit, :killed}}}},
                   250

    assert_receive {:DOWN, ^monitor, :process, ^caller, :normal}, 250
  end

  test "invalid portable timers are rejected before validator work starts" do
    test_pid = self()
    validator = fn endpoint -> send(test_pid, {:unexpected_validation, endpoint}) end
    unsafe_timeout = FerricStore.Timeout.max_finite() + 1

    for timeout <- [unsafe_timeout, -1, :invalid] do
      assert {:error, {:invalid_endpoint_validation_timeout, ^timeout}} =
               EndpointValidator.validate(validator, %{host: "cache.internal"}, timeout)
    end

    assert {:error, {:invalid_endpoint_validation_timeout, ^unsafe_timeout}} =
             EndpointValidator.validate(nil, %{host: "cache.internal"}, unsafe_timeout)

    refute_receive {:unexpected_validation, _endpoint}
  end
end
