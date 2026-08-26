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
- **leases** — a tuple can be bound to a pid via `Process.monitor` and is removed when that
  pid dies
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

## 5. Public API shape

The module `Tuplex` is the only public surface. Everything else is internal.

```elixir
Tuplex.out({:job, 1, "payload"})               # :ok
Tuplex.out({:lock, :db}, lease: self())        # removed when the caller dies

Tuplex.in({:job, :_, :_})                      # blocks: {:ok, tuple} | {:error, :timeout}
Tuplex.in({:job, :_, :_}, timeout: 5_000)
Tuplex.rd({:job, :_, :_})                      # blocks, leaves the tuple in place

Tuplex.inp({:job, :_, :_})                     # never blocks: {:ok, tuple} | :empty
Tuplex.rdp({:job, :_, :_})                     # never blocks: {:ok, tuple} | :empty
Tuplex.rd_all({:job, :_, :_})                  # [tuple] — possibly []

Tuplex.eval(fn -> {:result, expensive()} end)  # {:ok, pid}
Tuplex.watch({:job, :_, :_})                   # :ok — messages to the caller
```

`take/2` is a formatter-friendly alias for `in/2`, via
`defdelegate take(t, o), to: __MODULE__, as: :in`.

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
Tuplex                   public API, delegation + argument validation
  └─ Tuplex.Shard        GenServer, one per shard — owns blocking waiters, leases, watches
       └─ Tuplex.Store   pure functions over one ETS table — the ONLY module that touches :ets
  Tuplex.Template        pure — validation, key extraction, match-spec construction
  Tuplex.TableKeeper     owns the ETS tables so a Shard crash does not lose the space
```

Hard rule: **every `:ets` call lives inside `Tuplex.Store`.** `Shard` must never touch ETS
directly. `Store` stays process-free and independently unit-testable, which is what makes
the correctness-critical part of this system cheap to test.

### Storage form

Each shard owns one `:duplicate_bag` ETS table. The stored record is:

```elixir
{{tag, arity}, :erlang.unique_integer([:monotonic]), tuple}
```

The middle **uniqueness field is MANDATORY, not decorative.** Verified empirically on
OTP 28: in a `:duplicate_bag`, `:ets.delete_object/2` deletes **every** object equal to the
one given. Storing bare tuples and using `delete_object` to implement a destructive `in`
therefore removes *all* identical copies at once — insert a semaphore token three times,
take once, and the table size drops to 0. The uniqueness integer makes every record
distinct, so `delete_object` removes exactly one copy while identical user tuples still
coexist and count separately.

This is a silent data-loss trap that unit tests written over distinct tuples would never
catch. Test it with duplicates, explicitly.

Note the `:duplicate_bag` choice stands, but for a different reason than first assumed: the
key `{tag, arity}` is shared by many records, which is what rules out `:set`. Uniqueness
comes from the integer, not from the table type.

### Match specs

Built **by hand**. `:ets.fun2ms` cannot see runtime template values and is useless here.
The shape, verified on OTP 28:

```elixir
[{{{tag, arity}, :_, template}, [], [:"$_"]}]
```

Verified properties of this form:

- **arity is part of the match** — an arity-3 template never matches an arity-2 or arity-4
  tuple, because arity is baked into the key
- the template **doubles directly as the nested head pattern**; `:_` acts as the wildcard
- matching is **exact**: `1.0` does not match a stored `1`, which is correct for Linda
- `:ets.select(tab, ms, 1)` followed by `delete_object/2` takes exactly one copy of a
  duplicated tuple

## 7. Build order

**No step starts until the previous step's tests pass.** This is the whole discipline of
the project; there is no partial credit for a half-finished layer.

1. `Tuplex.Template` + `Tuplex.Store`, with full unit tests
2. `Tuplex.Shard` — `out`, `inp`, `rdp`
3. Blocking `in` / `rd` with the waiter index
4. Leases + `Tuplex.TableKeeper`
5. `watch`, `eval`, `rd_all`
6. Property tests (PropEr, stateful)
7. README and docs

## 8. Decisions taken while writing this file

These were not in the first session's notes and are open to correction:

- `inp/2` and `rdp/2` return `{:ok, tuple} | :empty`. `:empty` is a normal outcome, not an
  error, so it is not wrapped in `{:error, _}` the way `in/2`'s `:timeout` is.
- `:_` and any `:"$..."` atom are **reserved in templates**. `Template` rejects a template
  containing `:"$1"`-style atoms, because ETS would silently read them as match variables.
  They remain legal inside tuples passed to `out/2` — storage does not interpret content.
- `rd_all/2` returns a plain list, `[]` when nothing matches.
