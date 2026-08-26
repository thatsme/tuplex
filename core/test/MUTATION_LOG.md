# Mutation log

A passing test suite proves nothing about the suite. This records deliberate breaks made to
the implementation and whether the suite went red — run before publishing v0.1.

The entries that matter are the ones that **survived**. Two did, and only one of them was a
gap; the other turned out to be a mistake in what the docs claimed. Re-run this sweep when
you change `Shard.serve/4`, the lease settlement rules, or the retry predicate.

## Method

For each mutation: apply to `lib/`, `mix test`, record which tests fire, restore. No
tooling — `cp -r lib /tmp/pristine` and `perl -0pi -e` — because eight mutations is an hour
and [muzak](https://hex.pm/packages/muzak) is heavier than this needs.

## Results

| # | Break | Caught by |
| --- | --- | --- |
| M1 | Serve the `in` waiter before the `rd` waiters | **nothing** — see below |
| M2 | File waiters under the tag alone, not `Template.key/1` | **nothing** — correctly, see below |
| M2b | Look waiters up under a *narrower* key (`arity + 1`) | 25 tests, incl. the waiter property |
| M3 | Requeue on a `:normal` exit as well as abnormal | 2 — lease property, "a normal exit discards the tuple" |
| M4 | Skip `Process.monitor/1` on blocked callers | 3 — waiter leak test, counter-drift test, waiter property |
| M5 | Serve waiters before inserting the tuple | 8, incl. "an immediately taken tuple still consumes its sequence number" |
| M6 | Return the newest match from `inp` instead of the oldest | 8 across Store, Shard and the public API |
| M7 | Retry on any exit reason, not only `:noproc` | **nothing at the time** — now 3, see below |

## M1 — the ordering is unobservable, and that is the finding

`Shard.serve/4` delivers to matching `rd` waiters before handing the tuple to an `in`
waiter. Reversing that changes no behaviour and no test notices.

The reason is `deliver/4`: it sends the tuple **term already in hand**, and both the reader
and taker lists are computed before any delivery happens. Deleting the row cannot affect
what a reader receives. So the ordering has no observable consequence today.

This was documented in three places as though it were a guarantee. It is not, and those
places now say so. What *is* guaranteed — and is tested, in `waiter_test.exs` under
"readers are served before the taker" — is the user-facing outcome: a `rd` waiter matching a
tuple that an `in` waiter consumes in the same instant is still served.

The ordering is kept because it costs nothing and would become load-bearing the moment
delivery re-read the store instead of using the captured term. **If you change `deliver/4`
to re-read, this mutation stops being inert and needs a test.**

## M2 — the key is an index, not a decider

Bucketing waiters by tag alone is *semantically identical* to bucketing by
`Template.key/1`, just slower: `Template.matches?/2` is evaluated per waiter inside the
bucket and is what actually decides. So M2 surviving is correct behaviour, not a gap.

M2b is the mutation that matters, and it is well defended: a key that is **narrower** than
the match relation means waiters are never offered tuples they match, which is the
never-woken-`in` hang the key invariant property exists to prevent.

## M7 — testing the decision, not the race

Retrying a `GenServer.call/3` is safe only when the message provably never arrived, which
`:noproc` alone establishes. Any other exit reason may mean the shard handled the call and
died afterwards, so a retry would write the tuple twice.

Nothing tested that. The fix was not to test the race — that needs fault injection between
a shard receiving a message and replying, which is brittle enough to rot — but to extract
the predicate, `Shard.retryable_exit?/1`, and unit-test it directly. Reversing the decision
is now caught by three tests at no runtime cost.

**The double-write window itself remains untested.** That is a deliberate, recorded gap: the
window is narrow, the library is single-node and pre-1.0, and the alternatives were a
brittle trace-based test or an idempotency token on `out` that changes the wire protocol.

## Known untested areas

Recorded so they are not mistaken for coverage:

- The double-write window above.
- Shard restart under *concurrent* mixed load — callers mid-`in`, mid-`rdp`, and holding
  leases simultaneously. Each is tested alone; the interleaving is not.
- `Store.recover/1` is exercised against hand-built tables, not by a crash landing at the
  genuinely interrupted moment.
- Resource behaviour at scale — 100k tuples, 1k waiters — to confirm nothing is accidentally
  quadratic. The per-`out` waiter scan is the suspect.
- Sequence numbers as bignums after a reclaim.
