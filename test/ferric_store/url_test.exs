defmodule FerricStore.URLTest do
  use ExUnit.Case, async: true

  alias FerricStore.URL

  test "parses plaintext URLs with the native default port" do
    assert {:ok,
            %{
              host: "cache.internal",
              port: 6388,
              tls: false,
              username: nil,
              password: nil
            }} = URL.parse("ferric://cache.internal")
  end

  test "parses TLS URLs with the TLS default port and decoded credentials" do
    assert {:ok,
            %{
              host: "cache.internal",
              port: 6389,
              tls: true,
              username: "service user",
              password: "p@ss:word"
            }} = URL.parse("ferrics://service%20user:p%40ss%3Aword@cache.internal")

    assert {:ok, %{port: 6389, tls: true}} = URL.parse("ferric+tls://cache.internal")
  end

  test "password-only URLs select the default user" do
    assert {:ok, %{username: nil, password: "secret"}} =
             URL.parse("ferric://:secret@cache.internal")
  end

  test "rejects unsupported schemes, missing hosts, and invalid ports" do
    assert {:error, {:invalid_url_scheme, "http"}} = URL.parse("http://cache.internal")
    assert {:error, :invalid_url} = URL.parse("ferric:///missing-host")
    assert {:error, :invalid_url} = URL.parse("ferric://cache.internal:0")
  end

  test "rejects URL components that the native client cannot apply" do
    assert {:ok, %{host: "cache.internal"}} = URL.parse("ferric://cache.internal/")

    for url <- [
          "ferric://cache.internal/namespace",
          "ferric://cache.internal?timeout=10",
          "ferric://cache.internal#primary"
        ] do
      assert {:error, :invalid_url} = URL.parse(url)
    end
  end

  test "rejects oversized URLs before URI parsing" do
    url = "ferric://" <> String.duplicate("a", 1_000_000)
    {:reductions, before_parse} = Process.info(self(), :reductions)

    assert {:error, :invalid_url} = URL.parse(url)

    {:reductions, after_parse} = Process.info(self(), :reductions)
    assert after_parse - before_parse < 10_000
  end
end
