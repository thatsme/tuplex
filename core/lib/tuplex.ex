defmodule Tuplex do
  @moduledoc """
  A Linda tuple space for the BEAM.

  Processes coordinate by writing tuples into a shared store and reading them **by pattern
  rather than by address**, which decouples producer and consumer in time, space, and
  reference: neither has to be alive at the same moment, neither needs a reference to the
  other, and neither names the other at all. The tuple's shape is the whole contract.

      Tuplex.out({:job, 1, "payload"})
      Tuplex.inp({:job, :_, :_})
      #=> {:ok, {:job, 1, "payload"}}

  A tuple's first element is its **tag**, which must be a concrete atom: it selects the
  shard the tuple lives in. Templates use `:_` as a wildcard and match exactly otherwise —
  `1.0` never matches a stored `1`. See `Tuplex.Template` for the full matching rules,
  including how maps are handled.

  ## This is not a job queue

  If the caller already knows *who* does the work and *when*, Tuplex is the wrong tool:

  | If you need | Use | Why not Tuplex |
  | --- | --- | --- |
  | Durable, retried background jobs | Oban | Tuplex is in memory; a restart is an empty space |
  | Backpressured data pipelines | Broadway | Consumers pull when free; no demand flows upstream |
  | Topic fan-out to known subscribers | Phoenix.PubSub | Known subscribers each getting a copy is a broadcast |
  | To call one process and get an answer | GenServer | A request addressed to one server is not anonymous |

  Tuplex earns its place only where the coordination is genuinely anonymous and
  shape-driven. Used as a general-purpose queue it is a worse queue.

  ## Observability

  Every operation emits `:telemetry` events, and `in/2` and `rd/2` are spans whose duration
  is how long a consumer waited — the one number that says whether consumers are starved or
  producers are behind. See `Tuplex.Telemetry`, and read its note on `tag` cardinality
  before mapping any of it onto metric labels.

  ## Calling `in/2`

  Call it qualified — `Tuplex.in(template)` — which is what the examples here do and what
  the README recommends.

  The name collides with `Kernel.in/2`, the `x in list` operator, which is a macro and is
  auto-imported into every module. So `import Tuplex` fails: two `in/2`s are in scope and
  neither wins. `take/2` is an alias for `in/2` for anyone whose linter or editor objects to
  the qualified form.

  It *is* possible to import it, by un-importing the Kernel macro first, but it is a poor
  trade — four characters saved against `x in list` breaking everywhere else in the module,
  which is a confusing failure to debug:

      # not recommended
      import Kernel, except: [in: 2]
      import Tuplex

  ## Further reading

  Linda is Gelernter's, and the vocabulary here is deliberately his:

    * Gelernter, D. (1985). *Generative Communication in Linda.* ACM Transactions on
      Programming Languages and Systems 7(1), 80–112. The original: `out`, `in`, `rd`,
      `eval`, and the argument for decoupling in time, space and reference.
    * Carriero, N. and Gelernter, D. (1989). *Linda in Context.* Communications of the ACM
      32(4), 444–458. Answers the objections, and is the clearest statement of when a tuple
      space is and is not the right shape.
    * Hayes-Roth, B. (1985). *A Blackboard Architecture for Control.* Artificial
      Intelligence 26(3), 251–321. Where the `rd`-dominant workload that shaped this
      library's read path comes from.

  Tuplex departs from the papers in one respect that matters: leases. Classical Linda has no
  answer for a consumer that takes a tuple and then dies, and `in/2`'s `:lease` option is
  that answer.
  """

  alias Tuplex.Shard
  alias Tuplex.Template
  alias Tuplex.Watch

  @default_max_queue 10_000

  @doc """
  Writes `tuple` into the space.

  The tuple's tag selects its shard, which is started on demand. Raises `ArgumentError` if
  the tuple has no concrete atom tag.

  Identical tuples do not collapse: writing `{:token}` three times means three tuples, and
  three `inp/2` calls to drain them.

  ## Examples

      Tuplex.out({:job, 1, "payload"})
      #=> :ok
  """
  @spec out(Template.t()) :: :ok
  def out(tuple) do
    tuple = Template.validate_tuple!(tuple)
    {:ok, _seq} = Shard.out(tuple)
    :ok
  end

  @doc """
  Blocks until a tuple matching `template` is available, then removes and returns it.

  Returns `{:ok, tuple}`, or `{:error, :timeout}` if the wait runs out.

  ## Options

    * `:timeout` — milliseconds to wait, or `:infinity` (the default). `0` does not
      register a waiter at all; it behaves exactly like `inp/2`, reporting
      `{:error, :timeout}` where `inp/2` would say `:empty`.

    * `:lease` — `false` by default. `:monitor` or `{:monitor, :ack}` hold the tuple
      against the calling process rather than removing it outright, so it comes back if
      that process dies without finishing. See below.

  Exactly one blocked `in/2` receives any given tuple. Every `rd/2` waiting on a template
  that also matches is woken **first**, before this call consumes it, so a reader never
  ends up blocked on a tuple that has already been taken away.

  Waiters are served in the order they arrived.

  ## Leasing

  Without a lease, a tuple taken by a process that then crashes is simply gone. A lease
  keeps it in the space, marked as held, so that it comes back if the holder never finishes.
  There are two ways to say what "finished" means.

  ### `lease: {:monitor, :ack}`

  Returns `{:ok, tuple, handle}`. The tuple is held until you call `Tuplex.ack/1` with the
  handle, and **any** exit before that returns it to the space — a normal one included,
  because without an acknowledgement nothing says the work was done.

  This is the one to reach for by default. It is the only form that suits a worker taking
  many tuples over its life, because the lease ends when the work does rather than when the
  process does.

      {:ok, job, handle} = Tuplex.in({:job, :_}, lease: {:monitor, :ack})
      process(job)
      Tuplex.ack(handle)

  ### `lease: :monitor`

  Returns `{:ok, tuple}`, unchanged from an unleased take. The tuple is held for the
  **lifetime of the calling process**: a normal exit discards it, any other exit returns it.

  That makes it right for callers whose lifetime *is* the unit of work — a `Task` per tuple
  — and wrong for anything longer-lived. A worker that takes 500 tuples this way holds 500
  live leases, and a crash at the 501st requeues all of them, including the 499 it had
  already finished. Use `{:monitor, :ack}` there.

      Task.async(fn ->
        {:ok, job} = Tuplex.in({:job, :_}, lease: :monitor)
        process(job)
        # exiting normally here discards the tuple; crashing returns it to the space
      end)

  ### Both forms

  `:shutdown` and `{:shutdown, _}` requeue along with every other abnormal reason. A
  supervisor stopping a worker part-way through is orderly, but the work still did not
  happen.

  A requeued tuple goes to the **back** of the queue, not back to its original position.
  That is deliberate: at the front, a tuple that crashes whoever takes it would be handed
  straight back to the next taker in a tight loop. At the back it starves instead, which is
  visible rather than fatal.

  Leased tuples are invisible to `rd/2`, `rdp/1`, `rd_all/1` and other `in/2` callers while
  they are held.

  ## The name

  `in` is a binary operator in Elixir, so this is defined as `def unquote(:in)`. That is
  invisible at the call site, but it does mean `import Tuplex` fails: `Kernel.in/2` is a
  macro auto-imported everywhere, and two `in/2`s cannot both be in scope. Call it qualified,
  or use `take/2`. See the module docs.

  ## Examples

      Task.async(fn -> Tuplex.in({:job, :_}) end)
      Tuplex.out({:job, 1})
      #=> the blocked task returns {:ok, {:job, 1}}

      Tuplex.in({:nothing, :_}, timeout: 10)
      #=> {:error, :timeout}
  """
  @spec unquote(:in)(Template.template(), keyword()) ::
          {:ok, Template.t()} | {:ok, Template.t(), Shard.handle()} | {:error, :timeout}
  def unquote(:in)(template, opts \\ []) do
    template = Template.validate!(template)
    span(:in, template, timeout!(opts), lease!(opts))
  end

  @doc """
  Blocks until a tuple matching `template` is available, then returns it without removing
  it.

  Returns `{:ok, tuple}`, or `{:error, :timeout}` if the wait runs out. Takes the same
  `:timeout` option as `in/2`, where `0` behaves like `rdp/1`.

  **Every** waiting `rd/2` whose template matches an arriving tuple is woken, not just one
  — a non-destructive read has no reason to be exclusive.

  ## Examples

      Task.async(fn -> Tuplex.rd({:config, :_}) end)
      Tuplex.out({:config, %{mode: :fast}})
      #=> the blocked task returns {:ok, {:config, %{mode: :fast}}}, tuple still in place
  """
  @spec rd(Template.template(), keyword()) :: {:ok, Template.t()} | {:error, :timeout}
  def rd(template, opts \\ []) do
    template = Template.validate!(template)
    span(:rd, template, timeout!(opts), no_lease!(opts))
  end

  @doc """
  An alias for `in/2`, for callers who would rather not work around the operator name.
  """
  @spec take(Template.template(), keyword()) ::
          {:ok, Template.t()} | {:ok, Template.t(), Shard.handle()} | {:error, :timeout}
  defdelegate take(template, opts \\ []), to: __MODULE__, as: :in

  @doc """
  Removes and returns the oldest tuple matching `template`, without blocking.

  Returns `{:ok, tuple}`, or `:empty` if nothing matches right now.

  `:empty` is deliberately not `{:error, :empty}`. An empty space is an ordinary state for
  a tuple space to be in — unlike the timeout that the blocking `in/2` can fail with, which
  really is exceptional.

  This is the **exact** read: it is serialised through the tag's shard, so a tuple it
  returns was in the space and is now removed from it. No other consumer can also have it.

  ## Examples

      Tuplex.out({:job, 1})
      Tuplex.inp({:job, :_})
      #=> {:ok, {:job, 1}}
      Tuplex.inp({:job, :_})
      #=> :empty

  ## Options

    * `:lease` — as for `in/2`.
  """
  @spec inp(Template.template(), keyword()) ::
          {:ok, Template.t()} | {:ok, Template.t(), Shard.handle()} | :empty
  def inp(template, opts \\ []) do
    template = Template.validate!(template)
    Shard.take(template, lease!(opts))
  end

  @doc """
  Returns the oldest tuple matching `template` without removing it, and without blocking.

  Returns `{:ok, tuple}`, or `:empty` if nothing matches right now.

  Unlike `inp/2`, this runs **in the calling process** — a registry lookup and one ETS
  select, with no shard round-trip — so reads are fully parallel and never queue behind
  pending writes.

  The trade is freshness, and it is worth stating plainly rather than papering over: what
  you get back is a snapshot. Another process may take the tuple you just read before you
  do anything with it. If you need the tuple to be *yours*, use `inp/2`.

  ## Examples

      Tuplex.out({:job, 1})
      Tuplex.rdp({:job, :_})
      #=> {:ok, {:job, 1}}
      Tuplex.rdp({:job, :_})
      #=> {:ok, {:job, 1}}
  """
  @spec rdp(Template.template()) :: {:ok, Template.t()} | :empty
  def rdp(template) do
    template = Template.validate!(template)
    result = Shard.read(template)
    caller_event([:tuplex, :rdp], template, %{}, %{result: outcome_of(result)})
    result
  end

  @doc """
  Returns the tags that currently have a shard, in no particular order.

  A template's tag has to be a concrete atom, because the tag is what selects the shard.
  This is the door out of that for the cases that genuinely want the whole space —
  debugging, introspection, a deliberate sweep:

      Enum.flat_map(Tuplex.tags(), fn tag -> Tuplex.rdp({tag, :_, :_}) end)

  Written that way the fan-out is explicit and its cost is visible at the call site, which
  is exactly where it belongs. Note that arity is part of the match too, so a sweep still
  has to pick one — `tags/0` widens the tag, not the shape.
  """
  @spec tags() :: [atom()]
  defdelegate tags(), to: Shard

  @doc """
  Returns every tuple matching `template`, oldest first, leaving them all in place.

  Runs in the calling process, like `rdp/1`, and carries the same caveat: the list is a
  snapshot, and any of it may be taken by somebody else before you act on it. Identical
  tuples appear once each. Tuples currently held under a lease are not included.

  ## Examples

      Tuplex.out({:job, 1})
      Tuplex.out({:job, 2})
      Tuplex.rd_all({:job, :_})
      #=> [{:job, 1}, {:job, 2}]
  """
  @spec rd_all(Template.template()) :: [Template.t()]
  def rd_all(template) do
    template = Template.validate!(template)
    result = Shard.read_all(template)
    caller_event([:tuplex, :rd_all], template, %{matched: length(result)}, %{})
    result
  end

  @doc """
  Releases a tuple held under a `{:monitor, :ack}` lease.

  `handle` is the third element returned by `in/2` or `inp/2` in that mode. Returns `:ok`
  whether or not the lease is still held, so acknowledging twice, or acknowledging one that
  has already expired, is harmless rather than an error.
  """
  @spec ack(Shard.handle()) :: :ok
  defdelegate ack(handle), to: Shard

  @doc """
  Subscribes the calling process to tuples matching `template`, as they happen.

  Returns `{:ok, ref}`. Messages arrive as:

      {Tuplex.Shard, :watch, ref, event, tuple}

  Watching is **observational**, like `rd/2` and unlike `in/2`: a watcher never consumes,
  and every matching watcher hears about a tuple even when a blocked `in/2` takes it in the
  same instant. That works because a tuple is written to the table before anything is
  served, so what watchers are told about genuinely existed in the space.

  Unlike a blocking read, a subscription is standing: it stays until `unwatch/1`, until the
  subscriber dies, or until the node does. It survives its shard crashing.

  ## Options

    * `:events` — which events to receive, default `[:out]`. Also available are `:in`, for
      tuples being consumed, and `:requeue`, for leased tuples returning to the space after
      their holder failed. `:in` roughly doubles the volume on a busy tag and most watchers
      do not want it, which is why it is off by default.

    * `:max_queue` — drop threshold, default `#{@default_max_queue}`. See below.

  ## Watching is lossy, on purpose

  A watcher is pushed at, never asked, so nothing about how fast it can keep up reaches the
  shard. An unbounded send to a subscriber slower than the space is an unbounded mailbox
  that the shard can neither see nor recover from, and the node dies of it.

  So the subscriber's queue is measured before each send, and anything past `:max_queue` is
  **dropped**, with a `[:tuplex, :watch, :dropped]` telemetry event to say so. A debug
  console that quietly takes the node down with it is worse than one that misses events and
  tells you.

  If you need every tuple, you need `in/2` — a consumer that applies backpressure by
  existing — not a watcher.

  ## Examples

      {:ok, ref} = Tuplex.watch({:job, :_})
      Tuplex.out({:job, 1})
      #=> receives {Tuplex.Shard, :watch, ref, :out, {:job, 1}}
  """
  @spec watch(Template.template(), keyword()) :: {:ok, reference()} | {:error, term()}
  def watch(template, opts \\ []) do
    template = Template.validate!(template)
    {tag, _arity} = Template.key(template)

    spec = %{
      ref: make_ref(),
      tag: tag,
      template: template,
      events: events!(opts),
      subscriber: Keyword.get(opts, :subscriber, self()),
      max_queue: Keyword.get(opts, :max_queue, @default_max_queue)
    }

    with {:ok, _pid} <- Watch.start(spec), do: {:ok, spec.ref}
  end

  @doc """
  Ends the subscription `ref`. Returns `:ok` even if it has already ended.
  """
  @spec unwatch(reference()) :: :ok
  defdelegate unwatch(ref), to: Watch, as: :stop

  @doc """
  Runs `fun` in a fresh process and writes its result into the space.

  Returns `{:ok, pid}` immediately. This is Linda's `eval`: a way to say "the tuple is the
  result of this computation" without the caller waiting on it.

  > #### eval has no failure channel {: .warning}
  >
  > If `fun` raises, or returns something that is not a valid tuple, **nothing is written
  > and no one is told**. There is deliberately no error tuple: the arity of what `eval`
  > produces is whatever `fun` returns, so an error result would have no predictable shape
  > for a consumer to match against.
  >
  > A failure emits `[:tuplex, :eval, :exception]` telemetry and crashes its own process,
  > which is visible in logs and metrics but not in the space. So `eval/1` is for computing
  > a **tuple's contents**, not for work whose completion matters. If a consumer needs to
  > know the work happened, have it wait on the tuple with `in/2` and give the producer a
  > lease.

  ## Examples

      Tuplex.eval(fn -> {:report, expensive_calculation()} end)
      #=> {:ok, #PID<0.123.0>}
  """
  @spec eval((-> Template.t())) :: {:ok, pid()}
  def eval(fun) when is_function(fun, 0) do
    Task.Supervisor.start_child(Tuplex.EvalSupervisor, fn -> evaluate(fun) end)
  end

  # -- internals --------------------------------------------------------------

  # A single funnel per operation, so that step 6 can wrap each one in a telemetry span at
  # one call site instead of threading events through several early returns.
  # in/2 and rd/2 are the only spans, because their duration is the one operational number
  # nothing else exposes: how long a consumer waited. Everything else gets a discrete event,
  # since timing an ETS insert doubles the hot-path event volume for a number nobody reads.
  defp span(mode, template, timeout, lease) do
    {tag, arity} = Template.key(template)
    metadata = %{tag: tag, arity: arity, timeout: timeout, lease: lease}

    :telemetry.span([:tuplex, mode], metadata, fn ->
      result = blocking(mode, template, timeout, lease)
      {result, Map.put(metadata, :result, outcome(result))}
    end)
  end

  defp outcome({:error, :timeout}), do: :timeout
  defp outcome(_result), do: :ok

  defp outcome_of(:empty), do: :empty
  defp outcome_of(_result), do: :ok

  # Caller-side reads fire where the waiter index is not visible. The measurement set is
  # smaller rather than faked, because asking the shard for the rest would undo the whole
  # reason these reads bypass it.
  defp caller_event(event, template, measurements, metadata) do
    {tag, arity} = Template.key(template)

    :telemetry.execute(
      event,
      Map.merge(%{count: 1, space_size: Shard.space_size(tag)}, measurements),
      Map.merge(%{tag: tag, arity: arity}, metadata)
    )
  end

  defp blocking(mode, template, 0, lease) do
    # A zero timeout is the non-blocking probe. Registering a waiter only to sweep it in the
    # same breath would cost two shard calls and a monitor to reach the same answer.
    case probe(mode, template, lease) do
      {:ok, tuple} -> {:ok, tuple}
      {:ok, tuple, handle} -> {:ok, tuple, handle}
      :empty -> {:error, :timeout}
    end
  end

  defp blocking(mode, template, timeout, lease) do
    Shard.wait(mode, template, timeout, lease)
  end

  # The op is threaded through so the shard does not also emit an [:tuplex, :inp] event: this
  # call is already inside an in/2 span, and counting it twice would inflate any dashboard
  # that adds up operations.
  defp probe(:in, template, lease), do: Shard.take(template, lease, :in)
  defp probe(:rd, template, _lease), do: Shard.read(template)

  defp lease!(opts) do
    case Keyword.get(opts, :lease, false) do
      false ->
        false

      :monitor ->
        :monitor

      {:monitor, :ack} ->
        {:monitor, :ack}

      other ->
        raise ArgumentError,
              ":lease must be false, :monitor, or {:monitor, :ack}, got: #{inspect(other)}"
    end
  end

  # The failure is reported and then re-raised, so it lands in the logs as a crash as well
  # as in telemetry. A :kill cannot be caught and so goes unreported; nothing can help that.
  defp evaluate(fun) do
    tuple = fun.()
    out(tuple)

    {tag, arity} = Template.key(tuple)
    :telemetry.execute([:tuplex, :eval], %{count: 1}, %{tag: tag, arity: arity})
  catch
    kind, reason ->
      stacktrace = __STACKTRACE__

      :telemetry.execute(
        [:tuplex, :eval, :exception],
        %{count: 1},
        %{kind: kind, reason: reason, stacktrace: stacktrace}
      )

      :erlang.raise(kind, reason, stacktrace)
  end

  defp known_event?(:out), do: true
  defp known_event?(:in), do: true
  defp known_event?(:requeue), do: true
  defp known_event?(_other), do: false

  defp events!(opts) do
    case Keyword.get(opts, :events, [:out]) do
      events when is_list(events) and events != [] ->
        case Enum.reject(events, &known_event?/1) do
          [] -> events
          bad -> raise ArgumentError, "unknown watch events: #{inspect(bad)}"
        end

      other ->
        raise ArgumentError, ":events must be a non-empty list, got: #{inspect(other)}"
    end
  end

  defp no_lease!(opts) do
    if Keyword.has_key?(opts, :lease) do
      raise ArgumentError,
            ":lease applies to in/2 and inp/2 only — a non-destructive read takes nothing, " <>
              "so there is nothing to hand back"
    end

    false
  end

  defp timeout!(opts) do
    case Keyword.get(opts, :timeout, :infinity) do
      :infinity ->
        :infinity

      timeout when is_integer(timeout) and timeout >= 0 ->
        timeout

      other ->
        raise ArgumentError,
              ":timeout must be a non-negative integer or :infinity, got: #{inspect(other)}"
    end
  end
end
