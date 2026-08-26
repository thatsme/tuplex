defmodule Tuplex.Telemetry do
  @moduledoc """
  The `:telemetry` events Tuplex emits.

  This module is documentation and a list. It exists so the vocabulary lives in one place
  rather than being reconstructed by grepping for `:telemetry.execute/3`.

  > #### `tag` is unbounded {: .error}
  >
  > Every event carries the tuple's `tag` in its metadata, because a telemetry consumer
  > should be the one deciding what to do with it. But tags are **user-defined atoms**, and
  > mapping metadata straight onto Prometheus labels is the obvious thing to do.
  >
  > **Do not use `tag` as a metric label unless your tag set is fixed and small.** A tag
  > generated per entity — one atom per job id, per session, per user — becomes a time series
  > apiece, and that is how a time-series database dies. `arity` is safe: it is bounded
  > by the shapes your code actually writes, and it is genuinely useful for spotting a
  > template that never matches because it is the wrong size.
  >
  > Nothing here can enforce this. It is written down because whoever hits it will come
  > looking for exactly this paragraph.

  ## Where duration is the signal, and where it is not

  Only `in` and `rd` are spans. Their duration is **how long a consumer waited**, which is
  the answer to "are my consumers starved or are my producers behind" — the one operational
  question nothing else in the system exposes.

  Everything else is a single discrete event at its funnel point. `out` is an ETS insert and
  a waiter scan; timing it would double the event volume on the hot path to produce a number
  nobody can act on.

  ## Events

  ### Spans

  `[:tuplex, :in, :start | :stop | :exception]` and
  `[:tuplex, :rd, :start | :stop | :exception]`, with the measurements
  `:telemetry.span/3` provides — `system_time` on start, `duration` on stop.

  Metadata: `tag`, `arity`, `timeout`, `lease` (for `in`), and on stop `result`, which is
  `:ok` or `:timeout`.

  Spans carry no `space_size`, because `:telemetry.span/3` owns the measurements map. The
  space is observed properly by `[:tuplex, :shard, :stats]` instead.

  ### Operations

  | Event | Measurements | Metadata |
  | --- | --- | --- |
  | `[:tuplex, :out]` | `count`, `space_size`, `waiter_count` | `tag`, `arity` |
  | `[:tuplex, :inp]` | `count`, `space_size`, `waiter_count` | `tag`, `arity`, `result`, `lease` |
  | `[:tuplex, :rdp]` | `count`, `space_size` | `tag`, `arity`, `result` |
  | `[:tuplex, :rd_all]` | `count`, `space_size`, `matched` | `tag`, `arity` |
  | `[:tuplex, :eval]` | `count` | `tag`, `arity` |

  `result` is `:ok` or `:empty`.

  **`rdp` and `rd_all` carry no `waiter_count`, and that is not an oversight.** They execute
  in the calling process precisely so that a read never touches the shard, so their events
  fire where the waiter index is not visible. Adding it would mean a message to the shard on
  every read, which would undo the entire reason those reads bypass it. A smaller
  measurement set is the honest answer.

  `space_size` counts every row including leased ones, and is `:ets.info(tab, :size)` — O(1)
  wherever it is taken.

  ### Leases

  | Event | Measurements | Metadata |
  | --- | --- | --- |
  | `[:tuplex, :lease, :released]` | `count`, `space_size` | `tag`, `arity`, `mode` |
  | `[:tuplex, :lease, :requeued]` | `count`, `space_size` | `tag`, `arity`, `mode`, `reason` |

  A rising `:requeued` rate against a flat `:released` rate means consumers are dying
  holding work. `reason` is the holder's exit reason.

  ### Watches

  `[:tuplex, :watch, :dropped]`, measurements `count` and `message_queue_len`, metadata
  `tag`, `ref`, `event`, `template`.

  Watching is lossy by design — see `Tuplex.watch/2` — and this is how it says so. Any
  occurrence at all means a subscriber is slower than the space it is watching.

  ### Failures

  `[:tuplex, :eval, :exception]`, measurements `count`, metadata `kind`, `reason`,
  `stacktrace`.

  `eval/1` has no failure channel by design, so this event is the *only* place a failed
  computation is visible. See `Tuplex.eval/1`.

  ### Shard statistics

  `[:tuplex, :shard, :stats]`, emitted periodically by every shard.

  Measurements: `space_size`, `waiter_count`, `watch_count`, `lease_count`, and
  `oldest_waiter_age_ms`. Metadata: `tag`.

  This is the event that matters most and the one that corresponds to no operation at all.
  A shard whose `waiter_count` is climbing while `out` volume is flat is starving, and
  nothing in the per-operation events can tell you that. `oldest_waiter_age_ms` — the wait
  of the longest-blocked caller, or `0` with none — is the one to page on.

  It fires on a timer whether or not the shard is busy, so an idle shard reports zeros
  rather than a gap. Gaps in a series are harder to read than zeros.

  ### Configuration

      config :tuplex, stats_interval: 10_000   # milliseconds, or :off

  Ten seconds by default. A thousand shards is a hundred events a second, which is fine;
  a thousand shards on a one-second interval is its own problem. `:off` disables it.

  ## Handlers attached to shard-side events run inside the shard

  `[:tuplex, :out]`, `[:tuplex, :inp]`, the lease events and the stats event are emitted
  from the shard process, and `:telemetry` handlers are synchronous. **A slow handler
  attached to those blocks that tag entirely** — every write and every destructive read
  behind it. Keep them to arithmetic and a send. The span and caller-side events run in the
  calling process and can only slow their own caller.

  ## Attaching

      :telemetry.attach_many("my-handler", Tuplex.Telemetry.events(), &handle/4, nil)
  """

  @events [
    [:tuplex, :in, :start],
    [:tuplex, :in, :stop],
    [:tuplex, :in, :exception],
    [:tuplex, :rd, :start],
    [:tuplex, :rd, :stop],
    [:tuplex, :rd, :exception],
    [:tuplex, :out],
    [:tuplex, :inp],
    [:tuplex, :rdp],
    [:tuplex, :rd_all],
    [:tuplex, :eval],
    [:tuplex, :eval, :exception],
    [:tuplex, :lease, :released],
    [:tuplex, :lease, :requeued],
    [:tuplex, :watch, :dropped],
    [:tuplex, :shard, :stats]
  ]

  @doc """
  Every event name Tuplex emits, for `:telemetry.attach_many/4`.
  """
  @spec events() :: [[atom()]]
  def events, do: @events

  @doc false
  # The interval between [:tuplex, :shard, :stats] events, or :off.
  def stats_interval do
    Application.get_env(:tuplex, :stats_interval, 10_000)
  end
end
