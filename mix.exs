defmodule FerricStore.MixProject do
  use Mix.Project

  def project do
    [
      app: :ferricstore_sdk,
      version: "0.2.1",
      elixir: "~> 1.19",
      description: "Official Elixir SDK for FerricStore over the native ferric:// protocol.",
      package: package(),
      source_url: "https://github.com/ferricstore/ferricstore-elixir",
      homepage_url: "https://github.com/ferricstore/ferricstore-elixir",
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
      {:ex_doc, "~> 0.38", only: :dev, runtime: false}
    ]
  end

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
