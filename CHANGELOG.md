# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[semantic versioning](https://semver.org/spec/v2.0.0.html).

Only the `Tuplex` module is public API. `Tuplex.Telemetry` documents the event vocabulary
and is stable in the same sense. `Tuplex.Store` and `Tuplex.Template` are published for
reference but are **not covered by semantic versioning**; everything else is internal.

## [Unreleased]

Nothing yet.

## [0.1.0] — 2026-08-26

First release. A Linda tuple space for the BEAM, single-node.

### Added

- `out/1`, `in/2`, `rd/2`, `inp/2`, `rdp/1`, `rd_all/1` — the Linda operations, with
  `take/2` as an alias for `in/2` for callers who would rather not qualify the operator
  name.
- **Leases.** `in/2` and `inp/2` take `lease: :monitor` or `lease: {:monitor, :ack}`, which
  hold a tuple against the consuming process rather than removing it, so that a consumer
  that dies without finishing returns the tuple to the space instead of destroying it.
  This is the departure from classical Linda.
- `ack/1` to release a `{:monitor, :ack}` lease.
- `watch/2` and `unwatch/1` — standing, non-consuming subscriptions to `:out`, `:in` and
  `:requeue` events, surviving shard restarts. Lossy under backpressure by design.
- `eval/1` — compute a tuple's contents in a fresh process. No failure channel, by design.
- `tags/0` — the live shard tags, for explicit whole-space sweeps.
- Telemetry across the surface, with spans on `in/2` and `rd/2` and a periodic
  `[:tuplex, :shard, :stats]` per shard. See `Tuplex.Telemetry`.

### Notes

- Requires Elixir ~> 1.19 and OTP 28.
- Everything is in memory. A node restart is an empty space; there is no persistence.
- `inp/2`'s exactness is **shard-local** — it rests on one process per tag serialising its
  own destructive reads, and would not survive unchanged into a distributed version.
- `:_` can be stored but never selected specifically, since `:_` in a template is the
  wildcard. Documented as a limitation rather than rejected, because rejecting it correctly
  would mean a deep term walk on every write. See `Tuplex.out/1`.

[Unreleased]: https://github.com/thatsme/tuplex/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/thatsme/tuplex/releases/tag/v0.1.0
