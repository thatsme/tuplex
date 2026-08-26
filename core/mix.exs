defmodule Tuplex.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/thatsme/tuplex"

  @description """
  A Linda tuple space for the BEAM. Processes coordinate by writing tuples into a shared
  store and reading them by pattern rather than by address, decoupling producer and
  consumer in time, space, and reference.
  """

  def project do
    [
      app: :tuplex,
      version: @version,
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      description: @description,
      package: package(),
      docs: docs(),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Tuplex.Application, []}
    ]
  end

  defp deps do
    [
      {:telemetry, "~> 1.2"},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false},
      {:stream_data, "~> 1.1", only: [:dev, :test]},
      {:propcheck, "~> 1.4", only: [:dev, :test]}
    ]
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @source_url},
      files: ~w(lib mix.exs README.md LICENSE .formatter.exs)
    ]
  end

  defp docs do
    [
      main: "Tuplex",
      source_url: @source_url,
      source_ref: "v#{@version}",
      extras: ["README.md"]
    ]
  end
end
