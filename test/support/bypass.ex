defmodule Bypass do
  @moduledoc false

  defstruct [:port, :server, :state]

  def open do
    {:ok, state} = Agent.start(fn -> %{error: nil, expectations: []} end)

    {:ok, server} =
      Bandit.start_link(
        ip: {127, 0, 0, 1},
        plug: {Bypass.Plug, state},
        port: 0,
        startup_log: false
      )

    Process.unlink(server)
    {:ok, {_address, port}} = ThousandIsland.listener_info(server)
    bypass = %__MODULE__{port: port, server: server, state: state}
    ExUnit.Callbacks.on_exit({__MODULE__, make_ref()}, fn -> verify_and_close(bypass) end)
    bypass
  end

  def expect_once(%__MODULE__{} = bypass, method, path, callback) do
    add_expectation(bypass, method, path, callback, true)
  end

  def expect(%__MODULE__{} = bypass, method, path, callback) do
    add_expectation(bypass, method, path, callback, false)
  end

  def take_expectation(state, method, path) do
    Agent.get_and_update(state, fn current ->
      {matching, rest} =
        Enum.reduce(current.expectations, {nil, []}, fn expectation, {found, kept} ->
          consume_expectation(expectation, found, kept, method, path)
        end)

      {matching, %{current | expectations: Enum.reverse(rest)}}
    end)
  end

  def record_error(state, error, stacktrace) do
    Agent.update(state, fn current ->
      if current.error == nil do
        %{current | error: {error, stacktrace}}
      else
        current
      end
    end)
  end

  defp consume_expectation(expectation, found, kept, method, path) do
    cond do
      found != nil -> {found, [expectation | kept]}
      expectation.method != method -> {nil, [expectation | kept]}
      expectation.path != path -> {nil, [expectation | kept]}
      expectation.once -> {expectation, kept}
      true -> {expectation, [expectation | kept]}
    end
  end

  defp add_expectation(bypass, method, path, callback, once) do
    expectation = %{callback: callback, method: method, once: once, path: path}

    Agent.update(bypass.state, fn current ->
      %{current | expectations: current.expectations ++ [expectation]}
    end)

    :ok
  end

  defp verify_and_close(bypass) do
    result =
      if Process.alive?(bypass.state) do
        Agent.get(bypass.state, & &1)
      else
        %{
          error: {RuntimeError.exception("test HTTP server state stopped early"), []},
          expectations: []
        }
      end

    if Process.alive?(bypass.server), do: Supervisor.stop(bypass.server)
    if Process.alive?(bypass.state), do: Agent.stop(bypass.state)

    case result do
      %{error: {error, stacktrace}} ->
        reraise error, stacktrace

      %{expectations: expectations} ->
        unused = Enum.filter(expectations, & &1.once)

        if unused != [] do
          routes = Enum.map_join(unused, ", ", &"#{&1.method} #{&1.path}")
          raise "unused test HTTP expectations: #{routes}"
        end
    end
  end
end

defmodule Bypass.Plug do
  @moduledoc false

  def init(state), do: state

  def call(conn, state) do
    case Bypass.take_expectation(state, conn.method, conn.request_path) do
      nil ->
        error =
          RuntimeError.exception(
            "unexpected test HTTP request: #{conn.method} #{conn.request_path}"
          )

        Bypass.record_error(state, error, [])
        Plug.Conn.send_resp(conn, 500, Exception.message(error))

      expectation ->
        expectation.callback.(conn)
    end
  rescue
    error ->
      Bypass.record_error(state, error, __STACKTRACE__)
      Plug.Conn.send_resp(conn, 500, Exception.message(error))
  end
end
