defmodule FerricStore.MixProject do
  use Mix.Project

  def project do
    [
      app: :ferricstore_sdk,
      version: "0.4.0",
      elixir: "~> 1.20",
      description: "Official Elixir SDK for FerricStore over the native ferric:// protocol.",
      elixirc_paths: elixirc_paths(Mix.env()),
      package: package(),
      source_url: "https://github.com/ferricstore/ferricstore-elixir",
      homepage_url: "https://github.com/ferricstore/ferricstore-elixir",
      test_coverage: [
        summary: [threshold: 70],
        ignore_modules: [~r/^Inspect\.FerricStore\./]
      ],
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def cli do
    [
      preferred_envs: [
        credo: :test
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :ssl]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:arch_test, "~> 0.3.1", only: [:dev, :test], runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:ex_doc, "~> 0.38", only: :dev, runtime: false},
      {:jason, "~> 1.4"}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => "https://github.com/ferricstore/ferricstore-elixir",
        "FerricStore" => "https://github.com/ferricstore/ferricstore"
      }
    ]
  end
end
