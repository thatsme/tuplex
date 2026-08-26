defmodule Tuplex.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # Resolves a tag to its shard's pid *and* table reference. The table reference is what
      # lets non-destructive reads run in the calling process, and Tuplex.tags/0 is a
      # by-product of the same registry rather than a feature of its own.
      {Registry, keys: :unique, name: Tuplex.Registry},

      # Shards are started on demand by the first out/1 for a tag.
      #
      # The restart intensity is deliberately far above the default 3-in-5s. Shards are
      # independent of one another, so pooling their failures into one tight budget means a
      # handful of unrelated shard crashes takes down the supervisor and every other tag's
      # tuples with it — the blast radius of a crash should be one tag, not the space. The
      # cap is raised rather than removed so a shard that genuinely cannot start still gives
      # up instead of spinning.
      {DynamicSupervisor,
       name: Tuplex.ShardSupervisor, strategy: :one_for_one, max_restarts: 100, max_seconds: 5}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Tuplex.Supervisor)
  end
end
