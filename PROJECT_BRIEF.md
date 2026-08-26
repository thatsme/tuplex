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
2. `Tuplex.Shard` — `out`, `inp`, `rdp`
3. Blocking `in` / `rd` with the waiter index
4. Leases + `Tuplex.TableKeeper`
5. `watch`, `eval`, `rd_all`
6. Property tests (PropEr, stateful)
7. README and docs

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
