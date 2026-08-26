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

      # Heir to every shard table, so a shard crash costs a process rather than a space.
      # Started before the shards, since they hand it their tables at creation.
      Tuplex.TableKeeper,

      # Shards are started on demand by the first out/1 for a tag.
      #
      # The restart intensity is deliberately far above the default 3-in-5s. Shards are
      # independent of one another, so pooling their failures into one tight budget means a
      # handful of unrelated shard crashes takes down the supervisor and every other tag's
      # tuples with it — the blast radius of a crash should be one tag, not the space. The
      # cap is raised rather than removed so a shard that genuinely cannot start still gives
      # up instead of spinning.
      {DynamicSupervisor,
       name: Tuplex.ShardSupervisor, strategy: :one_for_one, max_restarts: 100, max_seconds: 5},

      # One process per watch subscription, holding the registration alive across shard
      # restarts. A watcher is not sitting in a call, so it cannot re-register for itself.
      {Registry, keys: :unique, name: Tuplex.WatchRegistry},
      {DynamicSupervisor,
       name: Tuplex.WatchSupervisor, strategy: :one_for_one, max_restarts: 100, max_seconds: 5},

      # eval/2's processes, supervised so a computation that crashes is reported rather than
      # silently orphaned.
      {Task.Supervisor, name: Tuplex.EvalSupervisor}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Tuplex.Supervisor)
  end
end
