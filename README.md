# Tuplex

A Linda tuple space for the BEAM. Processes coordinate by writing tuples into a shared store
and reading them **by pattern rather than by address** — so a producer never learns who
consumed its work, a consumer never learns who produced it, and neither has to be alive at
the same moment as the other.

```elixir
# somewhere
Tuplex.out({:job, 17, "resize thumbnails"})

# somewhere else, possibly later, possibly on a process that did not exist yet
{:ok, {:job, id, task}} = Tuplex.in({:job, :_, :_})
```

## The idea in one paragraph

`receive` is already a guarded command over your mailbox: you write patterns, and the
runtime hands you the first message that matches one. It is the most pleasant coordination
primitive on the BEAM, and it is locked to a single process — to use it, someone has to know
your pid.

A tuple space is that same guarded command over a store that everybody can reach. The
pattern is still the whole interface, but the mailbox is shared, so the sender no longer
needs to name the receiver. That one change is what lets consumers select work by *what they
are able to do* rather than by being told what to do.

## Consumers that select their own work

This is the case nothing else on the BEAM does well, so it goes first.

Suppose you are dispatching container moves to cranes. Cranes differ: reach, maximum weight,
whether they handle refrigerated units. With a queue you need a dispatcher that knows the
whole fleet, and every new crane capability is a change to the dispatcher.

With a tuple space, the dispatcher does not exist. Each crane asks for work it can actually
perform:

```elixir
# a small crane, reefer-capable
{:ok, move} = Tuplex.in({:move, :_, :light, :reefer})

# a heavy crane that does not do refrigerated units
{:ok, move} = Tuplex.in({:move, :_, :heavy, :dry})
```

and work is posted without any idea of who will take it:

```elixir
Tuplex.out({:move, "MSCU4823701", :heavy, :dry})
```

The match *is* the routing. Adding a crane with a new capability is a new template, not a
change to shared dispatch logic — nobody has to be told the fleet changed, because nobody
knew the fleet in the first place.

The same shape covers plugin dispatch, heterogeneous worker pools, capability negotiation,
and any case where "who can do this?" is a better question than "who should do this?".

## The classics

**A semaphore.** Permits are just tuples; taking one is `in`, returning one is `out`.

```elixir
for _ <- 1..3, do: Tuplex.out({:db_permit})

# a worker
{:ok, _} = Tuplex.in({:db_permit})       # blocks until a permit is free
query()
Tuplex.out({:db_permit})                 # hand it back
```

Identical tuples do not collapse — three `out`s mean three permits, and `in` removes exactly
one of them.

**A barrier.** Each worker announces arrival; the coordinator waits for all of them.

```elixir
# each worker
Tuplex.out({:arrived, worker_id})

# the coordinator
for _ <- 1..n, do: {:ok, _} = Tuplex.in({:arrived, :_})
IO.puts("everyone is here")
```

**A shared counter**, safely, without a GenServer: take the value, put back the successor.
Because `in` is destructive and serialised, no two processes can hold it at once.

```elixir
{:ok, {:count, n}} = Tuplex.in({:count, :_})
Tuplex.out({:count, n + 1})
```

**A latch that everyone reads.** `rd` is non-destructive, so a single tuple can unblock any
number of waiters at once.

```elixir
# many processes
{:ok, {:config, settings}} = Tuplex.rd({:config, :_})

# published once
Tuplex.out({:config, %{mode: :fast}})
```

## Exactly once, even when consumers crash

Every prior Linda has the same hole: a consumer that takes a tuple and then dies has
destroyed work with no record that it ever existed. Tuplex closes it with **leases**.

```elixir
{:ok, job, handle} = Tuplex.in({:job, :_}, lease: {:monitor, :ack})
process(job)
Tuplex.ack(handle)
```

The tuple is not removed when it is taken — it stays in the space, marked as held. It is
discarded only on `ack/1`. If the holder dies first, for any reason at all, the tuple goes
back and the next consumer gets it.

There is a second form, `lease: :monitor`, that binds the lease to the calling process's
lifetime rather than to an acknowledgement: a normal exit discards, anything else requeues.
It fits a `Task` per unit of work and nothing longer-lived, since a worker taking many tuples
that way accumulates a live lease for each. Reach for `{:monitor, :ack}` unless you know you
want the other.

A requeued tuple goes to the **back** of the queue, deliberately: at the front, a tuple that
crashes whoever takes it would be handed straight to the next taker in a tight loop.

`:shutdown` requeues along with every other abnormal exit. A supervisor stopping a worker
part-way through is orderly, but the work still did not happen.

## When not to use this

Tuplex is for coordination that is genuinely anonymous. If you already know *who* does the
work and *when*, something else is a better fit:

| If you need | Use | Why not Tuplex |
| --- | --- | --- |
| Durable, retried background jobs | [Oban](https://hex.pm/packages/oban) | Tuplex is in memory. A node restart is an empty space. |
| Backpressured data pipelines | [Broadway](https://hex.pm/packages/broadway) | Consumers pull when free; there is no demand signalling upstream. |
| Topic fan-out to known subscribers | [Phoenix.PubSub](https://hex.pm/packages/phoenix_pubsub) | If subscribers are known and every one gets a copy, that is a broadcast. |
| To call a specific process and get an answer | `GenServer` | A request addressed to one server is not anonymous coordination. |

Tuplex earns its place where the coordination is shape-driven and the participants do not
know each other. Used as a general-purpose queue it is a worse queue.

## Installation

```elixir
def deps do
  [{:tuplex, "~> 0.1.0"}]
end
```

Requires Elixir ~> 1.19 and OTP 28.

## Calling `in`

Call it qualified: `Tuplex.in({:job, :_})`. `take/2` is an alias if the qualified form
bothers your tooling.

`import Tuplex` does not work, and the reason is worth knowing: the clash is not with another
Tuplex function but with `Kernel.in/2` — the `x in list` operator — which is a macro
auto-imported into every module.[^import]

[^import]: You *can* import it, by un-importing the Kernel macro first
(`import Kernel, except: [in: 2]` and then `import Tuplex`). It is not recommended. Four
characters saved is a poor trade against `x in list` silently breaking everywhere else in
that module.

## Observability

Every operation emits `:telemetry`. `in` and `rd` are spans, so their duration is **how long
a consumer waited** — the number that answers whether consumers are starved or producers are
behind. Each shard also reports its depth, waiter count and oldest waiter age on a timer.

See `Tuplex.Telemetry` for the full vocabulary, and read its note on `tag` cardinality before
mapping any of it onto metric labels.

## Status

v0.1. The API is complete and covered by 266 tests including six properties — among them a
stateful model of the serial API and a property asserting that every tuple written is
consumed exactly once however many consumers are killed mid-work.

Out of scope for v0.1, deliberately: distribution across nodes, persistence, and the
higher-level blackboard abstractions the `tuplex_blackboard` name is reserved for.

## License

Apache-2.0.
