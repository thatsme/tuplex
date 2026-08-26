# Tuplex — Project Brief

> This file is the reference every build step reads. It was reconstructed from the design
> notes of the first session. If anything here contradicts what was actually agreed, this
> file is wrong and should be corrected — do not silently work around it.

Target: Elixir ~> 1.19, OTP 28. Developed against Elixir 1.19.5 / OTP 28 [erts-16.3].

---

## 1. What Tuplex is

Tuplex is a **Linda tuple space for the BEAM** — a coordination substrate where processes
communicate by writing tuples into a shared, content-addressable store and reading them
**by pattern rather than by address**.

It decouples producer and consumer along three axes:

- **time** — the writer and the reader need not overlap in life
- **space** — neither needs a reference to the other
- **reference** — neither names the other at all; the tuple's shape is the whole contract

## 2. What Tuplex is not

Tuplex is deliberately **not** a job queue, a stream processor, or a pub/sub bus. If the
caller already knows *who* does the work and *when*, they should reach for the right tool
instead:

| If you need | Use |
|---|---|
| Durable, retried background jobs | Oban |
| Backpressured data pipelines | Broadway / GenStage |
| Topic fan-out to known subscribers | Phoenix.PubSub |
| Named process lookup | Registry |

**The README must say this explicitly, near the top.** Tuplex earns its place only where
the coordination pattern is genuinely anonymous and shape-driven; overselling it as a
general replacement for the above is a failure mode, not a marketing win.

## 3. Repo layout

Two mix projects side by side in one repo. **Not an umbrella** — umbrellas complicate
independent publishing to hex, and these two will be published on different timelines.

```
tuplex/
  core/              # mix project :tuplex — v0.1 is ONLY this
  blackboard/        # mix project :tuplex_blackboard — path dep on core, intentionally EMPTY
  README.md
  PROJECT_BRIEF.md   # this file
```

`blackboard/` exists to reserve the name and the shape of the eventual higher-level
abstraction. It ships no code in v0.1 and its README says so plainly.

Hex name check (first session): `tessera` was **taken** — a live DZI deep-zoom package by
alexdont, 0.3.5, ~12k downloads. Hence the rename to Tuplex. Both `tuplex` and
`tuplex_blackboard` were free.

## 4. Scope

### In scope for v0.1

- `out/2` — write a tuple
- `in/2` — blocking destructive read
- `rd/2` — blocking non-destructive read
- `inp/2` — non-blocking destructive read
- `rdp/2` — non-blocking non-destructive read
- `rd_all/2` — non-destructive read of every match
- `eval/2` — evaluate a function in a fresh process, `out` its result
- `watch/2` — subscribe to tuples matching a template as they are written
- **leases** — `in`/`inp` can hold a tuple against the taking process via `Process.monitor`
  rather than removing it outright, returning it to the space if that process dies without
  finishing
- **tag-based shard partitioning** — the tuple's first element selects the shard
- **telemetry** — `:telemetry` events on the operations above

### Explicitly OUT of scope for v0.1

Do not scaffold these, do not stub them, do not leave TODO comments pointing at them.
An empty module named after a future feature is worse than nothing.

- distribution and node placement
- consistent hashing
- tuple centres / reaction rules
- blackboard abstractions (beyond the empty project)
- persistence of any kind
- hot-tag sub-sharding
- security / capability models

Scope discipline is a first-class requirement on this project, not a nice-to-have.

### Sketched for v0.2 — do not build

Recorded here only so the next design pass does not have to rediscover the shape.

**Ephemeral tuples.** `out({:crane_available, 3, caps}, ephemeral: true)` — a tuple that
vanishes when the process that declared it dies. Presence and service discovery: the tuple
*is* the declaration, so it should not outlive the declarer.

This is **not** a lease and the brief should never call it one. A lease requeues on death
because the work was not done; an ephemeral tuple is deleted on death because the claim it
represents is no longer true. Opposite behaviours.

It also does not disturb the arity trick that keeps leased rows invisible, because it must
not: an ephemeral tuple has to stay *readable* while its declarer lives, so it cannot be
marked in the row at all. It wants a side map in shard state, `seq => {ref, pid}`, with the
row left as a plain 2-tuple and `:DOWN` deleting by `seq`.

**Requeue counts**, for the poison-tuple detector, want the same mechanism for the same
reason: the count has to survive into a *free* row, and a free row cannot carry a third
element without breaking the read path. A side map keyed by `seq` is where both belong.

## 5. Public API shape

The module `Tuplex` is the only public surface. Everything else is internal.

```elixir
Tuplex.out({:job, 1, "payload"})               # :ok
Tuplex.in({:job, :_}, lease: :monitor)         # comes back if the taker dies
Tuplex.in({:job, :_}, lease: {:monitor, :ack}) # {:ok, tuple, handle}; Tuplex.ack(handle)

Tuplex.in({:job, :_, :_})                      # blocks: {:ok, tuple} | {:error, :timeout}
Tuplex.in({:job, :_, :_}, timeout: 5_000)
Tuplex.rd({:job, :_, :_})                      # blocks, leaves the tuple in place

Tuplex.inp({:job, :_, :_})                     # never blocks: {:ok, tuple} | :empty
Tuplex.rdp({:job, :_, :_})                     # never blocks: {:ok, tuple} | :empty
Tuplex.rd_all({:job, :_, :_})                  # [tuple] — possibly []

Tuplex.eval(fn -> {:result, expensive()} end)  # {:ok, pid} — no failure channel
Tuplex.watch({:job, :_, :_})                   # {:ok, ref} — messages to the caller

Tuplex.tags()                                  # [atom] — live shard tags
```

`inp/2` and `rdp/2` return `{:ok, tuple} | :empty`. The asymmetry with `in/2`'s
`{:error, :timeout}` is meaningful rather than sloppy, and the docs must say so: an empty
space is an ordinary state for a tuple space to be in, while a timeout is an exceptional
one. Do not "regularise" this to `{:error, :empty}`.

A template's tag must be a concrete atom — see §6. `tags/0` is the door out of that for the
debugging case: it returns the live shard tags, and a caller who genuinely wants a
whole-space sweep folds `rd_all/1` over it themselves. Fan-out then exists, its cost is
visible at the call site, and it never leaks into the hot API.

`take/2` is a formatter-friendly alias for `in/2`, via
`defdelegate take(t, o), to: __MODULE__, as: :in`.

**Arities arrive with the options that justify them.** As built, `out/1`, `inp/1` and
`rdp/1` take no options because none exist yet; `timeout:` arrives with the blocking reads
at step 3 and `lease:` with step 4. Adding a defaulted second parameter later
(`def out(tuple, opts \\ [])`) defines both arities and breaks no existing call, so there
is nothing to gain from carrying an ignored `opts` around in the meantime — and an ignored
parameter is exactly the kind of stub §4 forbids.

### The `in` naming decision

The public API keeps the Linda name `Tuplex.in/2`. This is awkward at the definition site
and the awkwardness is accepted deliberately — fidelity to the Linda vocabulary was judged
worth it, and the cost was measured before committing rather than assumed.

Plain `def in(template, opts)` is a **hard SyntaxError**: `in` is a binary operator, so the
parser reads `def(in(...))` and demands a left operand. The working idiom, verified on
Elixir 1.19.5 / OTP 28:

```elixir
@spec unquote(:in)(template(), keyword()) :: {:ok, tuple()} | {:error, :timeout}
def unquote(:in)(template, opts \\ []) do
```

Verified to work with this form: default arguments, `@doc` and `@spec` attachment, the
`defdelegate` alias, call sites both with and without opts, `x in list` still behaving
normally in other modules, and `mix format` leaving all of it untouched.

**Known cost:** `import Tuplex` clashes with `Kernel.in/2`. Callers need
`import Tuplex, except: [in: 2]`, or should alias rather than import. **This belongs in the
README.**

Do not "fix" the `unquote` form to a plain `def`. It will not compile.

## 6. Architecture

```
Tuplex                     public API, argument validation, delegation
  Tuplex.Registry          tag -> {shard pid, table ref}   (unique keys)
  Tuplex.ShardSupervisor   DynamicSupervisor, shards started on demand
    └─ Tuplex.Shard        GenServer per tag — seq counter, and from step 3 waiters,
       │                   leases, watches
       └─ Tuplex.Store     pure functions over one ETS table — the ONLY module touching :ets
  Tuplex.Template          pure — validation, key extraction, match-spec compilation
  Tuplex.TableKeeper       holds tables as heir so a Shard crash does not lose the space
```

Hard rule: **every `:ets` call lives inside `Tuplex.Store`.** `Shard` must never touch ETS
directly. `Store` stays process-free and independently unit-testable, which is what makes
the correctness-critical part of this system cheap to test.

### The registry is the lookup path, not a feature

`out` on an unseen tag has to resolve tag → pid before it can start a shard on demand, so
the registry is load-bearing regardless. `Tuplex.tags/0` is then a by-product of the same
table rather than something built for it.

The registry value carries **the table reference alongside the pid**, and that settles a
bigger question: whether reads go through the shard at all.

```elixir
Registry.register(Tuplex.Registry, tag, table_ref)
```

**They do not.** Serialising through a shard exists to stop two consumers taking the same
tuple — a property of *destructive* operations only. `rdp` and `rd_all` mutate nothing and
ETS reads are atomic per object, so a GenServer round-trip would buy no correctness while
costing a message hop plus head-of-line blocking behind every `out` already queued.

So: tables are **`:protected` with `read_concurrency: true`**, the shard is the sole
writer, and **non-destructive reads execute in the calling process** after a registry
lookup. The read path is one ETS lookup for the table reference plus one select, fully
parallel across schedulers. `in` / `inp` always go through the shard.

This matters more than it looks. The blackboard layer deferred out of v0.1 is `rd`-dominant
by nature — many knowledge sources examining the same hypotheses — and reads that
serialised on the tag's shard would bottleneck that layer on day one, on a decision made
here.

Two consequences, handled explicitly rather than papered over:

- **Stale table references.** A shard that dies takes its table with it, and a caller may
  hold the old reference. Reads rescue `ArgumentError`, re-resolve the tag **once**, and
  retry; a second failure propagates. One retry, not a loop.

  Re-resolving can return the *same* dead reference, because the registry's cleanup of a
  dead shard is asynchronous. Liveness decides: a dead shard means the table went with it
  and `:empty` is the honest answer; a live shard registered against a table it does not
  have is a bug and is left to raise.

- **Asymmetric semantics.** `rdp` is lock-free and returns a snapshot that may be stale on
  return; `inp` is serialised and exact. The docs say so plainly — the same honesty already
  applied to `inp`'s local exactness.

### Shard lifecycle

Shards are `:permanent` children of a `DynamicSupervisor` whose restart intensity is set
well above the default 3-in-5s. Shards are independent, so pooling their failures into one
tight budget means a few unrelated crashes take down the supervisor and every other tag's
tuples with it. The blast radius of a crash should be one tag, not the space. The cap is
raised rather than removed, so a shard that genuinely cannot start still gives up.

`tags/0` lags in both directions, since registration and unregistration both happen around
process lifecycle events. It is a snapshot, and documented as one.

### The sequence counter

Lives in the shard's **state**, not in ETS — it is serialised by construction, and
`:ets.update_counter` would only add a write per `out` for no gain.

It is initialised from the table, `:ets.last(tab) + 1`, not from zero. At v0.1 a crashed
shard loses its table and zero would be correct, but once `TableKeeper` (step 4) hands a
reclaimed table back, its rows are already numbered and a counter at zero would collide on
the first insert and trip `Store.insert/3`'s raise — the right failure, but a needless one.
Deriving from the table makes step 4 a no-op on this path. `:ets.last/1` returns
`:"$end_of_table"` on an empty table.

`Shard.out/1` returns `{:ok, seq}` even though `Tuplex.out/1` returns `:ok`: step 4's lease
requeue has to identify a specific row, and threading the sequence through from the start
is cheaper than retrofitting it.

### Storage form

Each shard owns one **`:ordered_set`** ETS table keyed by a monotonically increasing
sequence number, with the counter living in the shard's own state:

```elixir
{seq, tuple}
```

**`:duplicate_bag` keyed by `{tag, arity}` was the original plan and it is wrong.** A
destructive read has to remove **exactly one** of N identical rows — that is the whole
semaphore idiom, `out` a token three times and `in` it back one at a time — and a
`:duplicate_bag` has no primitive that does it:

- `:ets.delete_object/2` deletes **every** object equal to the one given, so taking one
  token drains all three at once. Verified on OTP 28.
- `:ets.take/2` is no escape: it takes **by key**, and in a per-tag shard the key is the
  tag, so a take removes the entire space.

Giving every row a distinct key solves it outright — removing exactly one row is
`:ets.delete(tab, seq)`.

Nothing is lost by the move. Within a shard every tuple carries the same tag **by
construction**, so a tag-keyed index selects everything and every match is a linear scan
whatever the table type. The key index buys nothing, which is what frees the choice of
table type to be made on other grounds. Two things are gained:

- **FIFO for free.** `:ordered_set` traversal follows key order, so the first match is the
  oldest match. Linda leaves the choice among matching tuples unspecified; predictable is
  strictly more useful than arbitrary, at no cost.
- O(log n) insert and delete, which is noise next to the O(n) scan they accompany.

The duplicate-drain trap is silent data loss that tests written over distinct tuples would
never catch. **Test it with identical tuples, explicitly.**

### Match specs

Built **by hand**. `:ets.fun2ms` cannot see runtime template values and is useless here.

The work is split so that `Template` never learns the storage form. `Template.compile/1`
returns a head shaped like the template plus any guards; `Store` nests that head under its
own key position and supplies the body:

```elixir
{head, guards} = Template.compile!(template)
[{{:_, head}, guards, [:"$_"]}]
```

- `Store` contributes only `:_` for the `seq` position, never a numbered variable, so
  `Template`'s `:"$1"`, `:"$2"`, … can never collide with anything `Store` introduces.
- The body `[:"$_"]` returns the whole record, so a destructive read reads the `seq` back
  out of the row it just matched and deletes by it.
- **Tag and arity stay in the head**, so they are matched structurally and cheaply.
- Matching is **exact**: `1.0` does not match a stored `1`, which is correct for Linda.
- Destructive and single reads use **`:ets.select(tab, ms, 1)`**, not select-everything-
  and-take-the-head. Early termination is what keeps a read on a shard holding real volume
  from walking the whole table.

### Maps in templates

Maps are ordinary Elixir terms and templates carry them, but they are **never placed in the
head**. ETS matches a map in a head *partially* — a `%{a: 1}` pattern matches a stored
`%{a: 1, b: 2}` — which would put a second, contradictory notion of "match" alongside the
exact one. Rejecting maps outright is a wider cut than the problem needs and would be a
day-one papercut on something as ordinary as `{:job, %{region: :north}}`.

So `compile/1` hoists the map-bearing subterm out of the head and pins it with an `=:=`
guard:

```elixir
# {:job, :_, %{region: :north}}  compiles to
{{:job, :_, :"$1"}, [{:"=:=", :"$1", {:const, %{region: :north}}}]}
```

`=:=` is exactly the equality the rest of the design promises. The hoist takes the largest
**wildcard-free** subterm containing the map, so one guard usually covers a whole nested
structure while wildcards elsewhere keep working.

What is rejected is the narrow case: **a wildcard inside a map**, `%{a: :_}`. That needs
the partial semantics back, nobody needs it in v0.1, and the error message is easy to
write.

Consequence for `$`-atoms: they are rejected only where they would reach the head. Inside a
hoisted subterm they land in a `{:const, _}` and are never interpreted, so
`{:cfg, %{name: :"$1"}}` is accepted.

### Waiters

`in` and `rd` block **in the calling process**, never inside the shard. Registering a
waiter is a fast call that either satisfies the read from the table or files the caller and
returns; the caller then sits in its own `receive`. A shard that blocked on a caller's
behalf would stop serving every other process using that tag.

Waiters are filed by `Template.key/1`. The key **narrows the candidate set; it does not
decide the match** — two waiters can share a key and hold different templates, so
`matches?/2` is still evaluated per waiter inside the bucket. Two waiters whose templates
both match one tuple necessarily share a key, since the tuple fixes both tag and arity;
that is the key invariant earning its keep.

**Readers before the taker.** When a tuple arrives, every matching `rd` waiter is woken
first, and only then is the tuple handed to exactly one `in` waiter, which consumes it.
Satisfying the taker first would delete the tuple while readers that legitimately matched
it were still blocked, and they would go on blocking for a tuple that had already been and
gone. Another silent hang.

**Insert before serve.** The tuple is inserted before any waiter is served, even when a
waiter takes it in the same breath: insert, serve the readers, then delete on behalf of the
taker. The other order leaves a window in which a concurrent caller-side read sees a gap
where the tuple never existed, and puts a hole in the sequence accounting.

**Arrival order among waiters.** Linda leaves the choice unspecified, and prepending to a
list would make service LIFO by accident — contradicting the FIFO the store already
guarantees for tuples. Buckets are stored newest-first because prepending is O(1), and
reversed when served. Deliberate, not whatever the cons cell produced.

**Blocked callers are monitored.** A caller that dies, or times out and walks away, would
otherwise leave a waiter that matches forever and silently swallows a tuple meant for a
live consumer — a leak that only shows up under load. Monitor on registration, drop on
`:DOWN`, demonitor on service or cancel.

**`timeout: 0` does not register a waiter.** It is the non-blocking probe, behaving exactly
as `inp` / `rdp` but reporting `{:error, :timeout}` in place of `:empty`. Registering a
waiter only to sweep it in the same breath would cost two shard calls and a monitor to
reach the same answer.

Two races that have to be handled rather than hoped away:

- **Timeout racing service.** A caller can time out in the instant the shard serves it, in
  which case the tuple has already left the space and dropping it would lose it outright.
  After the cancel is acknowledged the caller checks its mailbox once — both messages come
  from the shard, so ordering guarantees the tuple is already there if it was sent.
- **Shard death during a wait.** Blocking means "until a matching tuple arrives", and a
  crash does not change that contract. The caller monitors the shard and re-registers with
  its replacement rather than hanging forever or reporting a timeout that did not happen.

### Leases

`in(template, lease: true)` hands the tuple to the caller **without removing it**. The
tuple comes back if the caller dies without finishing, so a consumer crash costs a retry
rather than a tuple.

The trap is not the lease mechanism — that is `Process.monitor` plus a map — it is the
crash window between recording a lease and removing the row. Those are two writes and they
are not atomic, and every ordering has a failure mode:

- **Record the lease first, then delete the row.** A crash in between leaves the row
  present *and* a lease claiming it needs requeuing. Duplicate delivery.
- **Delete the row first, then record.** A crash in between loses the tuple with no record
  it ever existed. Silent loss — the failure this library exists not to have.

The way out is **not to delete the row at all**. It is marked in place:

```elixir
{seq, tuple}                          # free
{seq, tuple, {:leased, ref, pid}}     # held
{seq, tuple, {:requeueing, new_seq}}  # a requeue caught mid-flight
```

On an `:ordered_set` a single `:ets.insert/2` replaces the row under the same key, so the
free-to-leased transition is one operation with no window at all. Requeueing is another
single insert stripping the third element. And recovery after a shard crash is a select for
marked rows, which is **authoritative because the table is the record** — there is no
second structure to reconcile against and no way for the two to disagree.

The cost is that reads must exclude leased rows, and it turns out to be free: a leased row
has arity 3, and the arity-2 head pattern every read already uses cannot match it. Not even
a guard, and `Template` still knows nothing about any of it.

**`rd` does not see leased tuples.** Arguable — a lease is a claim, not a deletion. But a
leased tuple will either be consumed or requeued, and exposing an in-flight claim would
make `rd` results depend on consumer timing. Excluded, and documented.

**Requeue goes to a fresh sequence, at the back.** Reinserting at the original sequence
would preserve arrival order, which is fairer for job dispatch, but a tuple that crashes
whoever takes it would then be handed straight back to the next taker in a tight loop. At
the back the same poison tuple starves rather than stalls — visible instead of fatal. A
requeue count for a later poison detector would have to survive the requeue, meaning it
would have to live in the *free* row, which changes the arity guard. Deliberately deferred
rather than half-built.

**Only `:normal` discards** — in `:monitor` mode. Every other exit reason requeues,
`:shutdown` and `{:shutdown, _}` included: a supervisor stopping a worker mid-lease is
orderly, but the work still did not happen.

### Two lease modes, because process lifetime is not always the unit of work

`lease: :monitor` binds the lease to the caller's lifetime. That is right when the caller's
life *is* the piece of work — a `Task` per tuple — and quietly wrong for anything
longer-lived. A worker taking 500 tuples over its life holds 500 live leases, and a crash at
the 501st requeues all of them, 499 of which were finished. That is not a papercut but a
silent violation of the exactly-once claim the library rests on, and "use a Task per tuple"
is a workaround that puts the burden on whoever did not read that far.

So `lease: {:monitor, :ack}` returns `{:ok, tuple, handle}` and holds the tuple until
`Tuplex.ack/1`. **Any** exit before that requeues, a normal one included, because without
an acknowledgement nothing says the work was done. Varying the return shape by an option
that literally says `:ack` is defensible; changing `:monitor`'s shape would not have been,
so it is untouched.

The mode travels in the row, not just in shard state, so a shard reclaiming a table after a
crash knows how each held tuple is meant to be settled.

### The three-write requeue

Requeueing at a fresh sequence is the one place that needs more than one write, so the
order makes it recoverable rather than atomic: mark the intent at the old key, publish the
tuple at the new key, retire the old row. `Store.recover/1` finishes whatever was
interrupted, idempotently, by checking whether the tuple already reached its new position.

### `Tuplex.TableKeeper`

Heir to every shard table. A shard's tuples now outlive its process: the table is handed to
the keeper on death and claimed back by the replacement, which then recovers the leases
recorded in it — re-monitoring holders that are still alive and requeueing those that are
not.

The keeper deliberately does almost nothing, because tables whose heir is a dead process
die with it. Its one job is staying alive, and the way to guarantee that is to have no
logic that could fail.

One window is at-least-once rather than exactly-once and cannot be closed: a holder that
finished and exited normally *while the shard was down* leaves no record of why it exited,
so its tuple is requeued as though the work had failed. Losing it instead would be worse.

### Watching

`watch/2` files a template in shard state alongside the waiters, matched at the same funnel
point, so ordering comes for free. A watch is **non-consuming and persistent**: it does not
take, and it stays until `unwatch/1`, the subscriber dies, or the node does.

Watchers are observational like `rd`, so they hear about a tuple even when an `in` waiter
consumes it in the same instant — the insert-before-serve rule is what makes that true
rather than a lie.

Three things this needs that a blocking read does not:

- **Surviving shard death.** A watcher is not sitting in a call, so it cannot re-register
  the way a blocked caller does. `Tuplex.Watch` is a small supervised process per
  subscription that holds the ref, monitors both the shard and the subscriber, and registers
  again with the replacement. Events go from shard to subscriber directly; the Watch process
  is only the registration's keeper, so a slow subscriber cannot back up behind it.

- **Backpressure, or the honest lack of it.** A watcher is pushed at and never asked, so an
  unbounded send to a slow subscriber is an unbounded mailbox the shard cannot see. The
  subscriber's queue is measured before each send and anything past `:max_queue` is dropped,
  with `[:tuplex, :watch, :dropped]` telemetry. **Watching is lossy and documented as
  lossy.** A debug console that quietly takes the node down with it is worse than one that
  misses events and says so. A caller that needs every tuple needs `in/2` — a consumer that
  applies backpressure by existing.

- **A small event set.** `events: [:out]` by default, with `:in` and `:requeue` available.
  `:in` roughly doubles the volume on a busy tag and most watchers do not want it.

### `eval` has no failure channel

If the function raises, nothing is written and nobody is told. That is not an oversight and
it must not be patched with an error tuple: the arity of what `eval` produces is whatever
the function returns, so an error result would have no predictable shape for a consumer to
match against.

Instead the failure emits `[:tuplex, :eval, :exception]` and crashes its own supervised
process, which is visible in logs and metrics but not in the space. `eval/1` is for
computing a **tuple's contents**, not for work whose completion matters — which is what it
was in Linda too. A consumer that needs to know the work happened should wait on the tuple
and the producer should hold a lease.

### The key invariant

```
if matches?(template, tuple) then key(template) == key(tuple)
```

`Shard` files waiting templates by `key/1` and offers a newly written tuple only to the
waiters under the tuple's own key. If this ever fails, `out` looks in the wrong bucket and
a legitimately waiting `in` is never woken — a hang, not a crash, and the worst thing in
this codebase to debug. **Property-tested**, not merely exampled.

## 7. Build order

**No step starts until the previous step's tests pass.** This is the whole discipline of
the project; there is no partial credit for a half-finished layer.

1. `Tuplex.Template` + `Tuplex.Store`, with full unit tests — **done**
2. `Tuplex.Shard` + `Tuplex.Registry` — `out`, `inp`, `rdp`, `tags` — **done**
3. Blocking `in` / `rd` with the waiter index — **done**
4. Leases + `Tuplex.TableKeeper` — **done**
5. `watch`, `eval`, `rd_all` — **done**
6. **Telemetry**, as one pass over the complete surface
7. Property tests (PropEr, stateful)
8. README and docs

Telemetry gets its own step rather than being folded into each operation as it is built.
It is cross-cutting: adding it per-op means revising event names and measurement shapes
with every new operation, so the retroactive pass happens either way, just spread thinner
and with a less coherent vocabulary at the end. One pass over a finished surface produces
one vocabulary.

What that costs now is a discipline, not code: **every operation must have a single funnel
point where a span can wrap it**, rather than several early returns per function. Each
public function in `Tuplex` therefore validates and then makes exactly one call to an
internal function that produces the result.

## 8. Settled decisions

All ratified; no longer open.

- **`inp/2` / `rdp/2` return `{:ok, tuple} | :empty`.** Not `{:error, :empty}` — see §5.
- **A template's tag must be a concrete atom.** The moment a wildcard tag is legal,
  partitioning stops being a partition. `Tuplex.tags/0` is the explicit door for the debug
  case.
- **Maps are supported in templates via `=:=` guards**, and only a wildcard *inside* a map
  is rejected. See §6.
- **`rd_all/2`** returns a plain list, `[]` when nothing matches.
- **License: Apache-2.0.** The patent grant is worth having for anything that might end up
  inside an enterprise, and Apache-2.0 clears that kind of legal review without a
  conversation. Elixir itself is Apache-2.0. The canonical `LICENSE` is at the repo root
  and is copied into `core/` and `blackboard/` so each published package ships its own.

## 9. Repo conventions

- **Line endings are LF.** The root `.gitattributes` sets `* text=auto eol=lf`, which
  overrides a contributor's global `core.autocrlf`. Do not commit CRLF.
- `git remote origin` is `https://github.com/thatsme/tuplex.git` (private).
- No placeholder code. The generator's `Tuplex.hello/0` was deleted rather than left to be
  screenshotted; `Tuplex` carries a moduledoc and nothing else until it has real API to
  carry.
