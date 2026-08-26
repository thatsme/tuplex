defmodule Tuplex.Shard do
  @moduledoc """
  One GenServer per tag, owning that tag's table, its sequence counter, and the processes
  blocked waiting for a tuple that has not arrived yet.

  Shards are started on demand: the first `out/1` for a tag resolves the tag to a pid and
  starts a shard if there is none. Resolution goes through `Tuplex.Registry`, whose value
  carries the shard's **table reference** alongside its pid:

      Registry.register(Tuplex.Registry, tag, table_ref)

  That extra field is what makes the read path cheap, and `tags/0` falls out of the same
  registry for free rather than being a feature of its own.

  ## Destructive operations are serialised; reads are not

  Serialising through the shard exists to stop two consumers taking the same tuple. That is
  a property of *destructive* operations only. `read/1` and `read_all/1` mutate nothing and
  ETS reads are atomic per object, so a GenServer round-trip would buy no correctness while
  costing a message hop and head-of-line blocking behind every `out` already in the
  mailbox.

  So non-destructive reads run **in the calling process**: one ETS lookup in the registry
  for the table reference, then one select. No shard involvement, fully parallel across
  schedulers. `out/1` and `take/1` always go through the shard.

  This is not a micro-optimisation. The blackboard layer this library is eventually for is
  `rd`-dominant by nature — many knowledge sources examining the same hypotheses — and a
  read path that serialised on the tag's shard would bottleneck that layer on day one, on a
  decision made here.

  ## Two consequences, stated rather than papered over

  **Stale table references.** A caller can hold a reference to a table whose shard has died,
  in which case `:ets` raises `ArgumentError`. Reads catch it, re-resolve the tag **once**,
  and retry; a second failure propagates. One retry, not a loop — a shard that cannot stay
  up should surface as an error, not as an invisible spin.

  Re-resolving can hand back the *same* dead reference, because the registry's cleanup of a
  dead shard is asynchronous and a read can land inside that window. Retrying there would
  only raise the same error again, so the shard's liveness decides: a dead shard means the
  reference cannot be reached from here and an empty space is the honest answer, while a
  live shard registered against a table it does not have is a bug and is left to raise.

  In practice this path is rare now that `Tuplex.TableKeeper` inherits a dead shard's table.
  A table survives its shard, keeping the same reference, so a caller holding one across a
  crash usually keeps reading successfully and the replacement reclaims the very same table.
  The retry covers what is left: a table genuinely destroyed, which needs the keeper to have
  gone too.

  **Asymmetric freshness.** `take/1` is serialised and exact: what it returns was in the
  space and is now yours. `read/1` is lock-free and returns a snapshot that may already be
  stale by the time the caller sees it — a tuple it found can be taken by someone else
  immediately after. That is the honest description of a lock-free read.

  ## Waiters

  `wait/3` blocks until a matching tuple arrives. The blocking happens **in the calling
  process**, never inside the shard: registering a waiter is a fast call that either
  satisfies the read from the table or files the caller in the waiter index and returns, at
  which point the caller sits in its own `receive`. A shard that blocked on behalf of a
  caller would stop serving every other process using that tag.

  Waiters are filed by `Tuplex.Template.key/1`, so a newly written tuple is only offered to
  waiters under its own key. The key **narrows the candidate set; it does not decide the
  match** — two waiters can share a key and hold different templates, so `matches?/2` is
  still evaluated per waiter within the bucket.

  ### Readers are served before the taker

  When a tuple arrives, **every** matching `rd` waiter is woken first, and only then is the
  tuple handed to exactly one `in` waiter, which consumes it.

  The order is not cosmetic. Satisfying the `in` first would delete the tuple while `rd`
  waiters that legitimately matched it were still blocked — and they would go on blocking,
  waiting for a tuple that has already been and gone. A silent hang, which is the failure
  mode this codebase works hardest to avoid.

  For the same reason the tuple is **inserted before any waiter is served**, even when a
  waiter takes it in the same breath: insert, serve the readers, then delete on behalf of
  the taker. Writing it the other way would leave a window in which a concurrent
  caller-side read sees a gap where the tuple never existed, and would put a hole in the
  sequence accounting.

  ### Waiters are served in arrival order

  Linda leaves the choice among waiters unspecified, and prepending to a list would make
  service LIFO by accident. That would contradict the FIFO the store already guarantees for
  tuples, so buckets are stored newest-first — prepending is O(1) — and reversed when
  served. Predictable beats nondeterministic when it is free.

  ### Blocked callers are monitored

  A caller that dies, or that times out and walks away, would otherwise leave a waiter that
  matches forever and silently swallows a tuple meant for a live consumer. Every waiter is
  monitored on registration, dropped on `:DOWN`, and demonitored when served or cancelled.
  It is a leak that only shows up under load.
  """

  use GenServer

  alias Tuplex.Store
  alias Tuplex.TableKeeper
  alias Tuplex.Template

  @registry Tuplex.Registry
  @supervisor Tuplex.ShardSupervisor

  # The table is not :named_table, so this label is only for identification in :ets.i/0 and
  # crash dumps. A constant keeps shard creation from minting a fresh atom per tag.
  @table_label :tuplex_shard

  defstruct [
    :tag,
    :tab,
    :seq,
    waiters: %{},
    waiter_index: %{},
    watches: %{},
    watch_index: %{},
    leases: %{},
    # mref => {:waiter | :watch | :lease, ref}. One map for every monitor the shard holds,
    # tagged by what it is watching, so a :DOWN needs one lookup rather than three misses.
    monitors: %{}
  ]

  @typedoc "The tag a shard is responsible for."
  @type tag :: atom()

  @typedoc "Whether a waiter consumes the tuple it is given."
  @type mode :: :in | :rd

  @typedoc """
  How a taken tuple is held.

  `false` removes it outright. `:monitor` holds it for the caller's lifetime, discarding on
  a normal exit and requeueing on any other. `{:monitor, :ack}` holds it until
  `Tuplex.ack/1`, requeueing on *any* exit that arrives first — including a normal one,
  since without an acknowledgement there is nothing to say the work was finished.
  """
  @type lease :: false | :monitor | {:monitor, :ack}

  @typedoc "What a watcher is told about."
  @type event :: :out | :in | :requeue

  @typedoc "An opaque handle to a held tuple, for `ack/1`."
  @opaque handle :: {tag(), reference()}

  @doc """
  Releases a held tuple: the work it represents is finished.

  Only meaningful for a `{:monitor, :ack}` lease, where nothing else discards the tuple.
  Returns `:ok` whether or not the lease is still held, so a duplicate acknowledgement is
  harmless and one racing an expiry does not raise.
  """
  @spec ack(handle()) :: :ok
  def ack({tag, ref}) do
    call_existing(tag, {:ack, ref}, :ok)
  end

  @doc false
  def register_watch(tag, ref, template, events, subscriber, max_queue) do
    call_shard(tag, {:watch, ref, template, events, subscriber, max_queue})
  end

  @doc false
  def unregister_watch(tag, ref) do
    call_existing(tag, {:unwatch, ref}, :ok)
  end

  @doc false
  def start_link(tag) when is_atom(tag) do
    GenServer.start_link(__MODULE__, tag, name: via(tag))
  end

  @doc """
  Returns the tags with a live shard, in no particular order.

  This is the sanctioned way to fan out across the whole space: a template's tag must be
  concrete, so a caller who genuinely wants every tag folds over this list themselves. The
  cost is then visible at the call site instead of hidden in the hot path.

  The list is a snapshot and lags in both directions: a shard that has just died is
  unregistered when the registry processes its exit, so a tag can appear here for a moment
  after its tuples are gone.
  """
  @spec tags() :: [tag()]
  def tags do
    Registry.select(@registry, [{{:"$1", :_, :_}, [], [:"$1"]}])
  end

  @doc """
  Writes `tuple` into its tag's shard, starting the shard if there is none.

  Returns `{:ok, seq}`. The public API discards the sequence number, but leases need to
  identify the exact row they wrote, so it is threaded through from here.

  Any waiters the tuple satisfies are served before this returns.
  """
  @spec out(Template.t()) :: {:ok, Store.seq()}
  def out(tuple) do
    {tag, _arity} = Template.key(tuple)
    call_shard(tag, {:out, tuple})
  end

  @doc """
  Removes and returns the oldest tuple matching `template`, via its tag's shard.

  Returns `:empty` when nothing matches, and does not start a shard: a tag with no shard
  has nothing to take.
  """
  @spec take(Template.template(), lease()) ::
          {:ok, Template.t()} | {:ok, Template.t(), handle()} | :empty
  def take(template, lease \\ false) do
    {tag, _arity} = Template.key(template)
    call_existing(tag, {:take, template, lease}, :empty)
  end

  @doc """
  Returns the oldest tuple matching `template`, leaving it in place.

  Runs in the calling process. Returns `:empty` when nothing matches or the tag has no
  shard.
  """
  @spec read(Template.template()) :: {:ok, Template.t()} | :empty
  def read(template) do
    in_table(template, :empty, &Store.read(&1, template))
  end

  @doc """
  Returns every tuple matching `template`, oldest first, leaving them in place.

  Runs in the calling process. Returns `[]` when nothing matches or the tag has no shard.
  """
  @spec read_all(Template.template()) :: [Template.t()]
  def read_all(template) do
    in_table(template, [], &Store.read_all(&1, template))
  end

  @doc """
  Blocks the calling process until a tuple matching `template` arrives.

  `mode` is `:in` to consume the tuple or `:rd` to leave it in place. Returns
  `{:ok, tuple}`, or `{:error, :timeout}` once `timeout` milliseconds have passed;
  `:infinity` waits indefinitely.

  Unlike `take/1`, this starts a shard for an unseen tag — the caller needs somewhere to
  wait, and a tuple may well arrive later.

  The wait happens here, in the caller. The shard is only asked to file the waiter, which
  it answers immediately, either with a tuple already in the table or with an
  acknowledgement that the caller is now registered.
  """
  @spec wait(mode(), Template.template(), timeout(), boolean()) ::
          {:ok, Template.t()} | {:error, :timeout}
  def wait(mode, template, timeout, lease? \\ false) do
    wait_until(mode, template, deadline(timeout), lease?)
  end

  @doc """
  Resolves a tag to `{:ok, pid, table}`, or `:error` if no shard is running for it.
  """
  @spec lookup(tag()) :: {:ok, pid(), Store.tab()} | :error
  def lookup(tag) do
    case Registry.lookup(@registry, tag) do
      # A shard registers its name before init/1 runs and fills the table in from inside it,
      # so a lookup racing a starting shard can see nil. Asking the shard resolves it: the
      # call is served only once init/1 has returned.
      [{pid, nil}] -> {:ok, pid, GenServer.call(pid, :table)}
      [{pid, tab}] -> {:ok, pid, tab}
      [] -> :error
    end
  end

  @doc """
  Starts a shard for `tag` unless one is already running.
  """
  @spec ensure(tag()) :: {:ok, pid()} | {:error, term()}
  def ensure(tag) do
    case lookup(tag) do
      {:ok, pid, _tab} ->
        {:ok, pid}

      :error ->
        case DynamicSupervisor.start_child(@supervisor, {__MODULE__, tag}) do
          {:ok, pid} -> {:ok, pid}
          # Lost the race to another caller starting the same tag.
          {:error, {:already_started, pid}} -> {:ok, pid}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  @doc false
  # The stale-reference retry, exposed so it can be tested with a deliberately dead table.
  # Callers should use read/1 and read_all/1.
  def attempt(tag, tab, default, fun) do
    fun.(tab)
  rescue
    ArgumentError -> reread(tag, tab, default, fun)
  end

  # -- waiting, in the calling process ----------------------------------------

  defp wait_until(mode, template, deadline, lease?) do
    {tag, _arity} = Template.key(template)

    with {:ok, pid} <- ensure(tag) do
      ref = make_ref()
      mref = Process.monitor(pid)
      outcome = register_and_block(pid, mref, mode, template, ref, deadline, lease?)
      Process.demonitor(mref, [:flush])

      case outcome do
        # The shard died holding our registration. Blocking means "until a matching tuple
        # arrives", and a crash does not change that contract, so file with its replacement
        # rather than leaving the caller hung or lying about a timeout.
        :shard_died -> retry(mode, template, deadline, lease?)
        result -> result
      end
    end
  end

  defp retry(mode, template, deadline, lease?) do
    case remaining(deadline) do
      0 -> {:error, :timeout}
      _ -> wait_until(mode, template, deadline, lease?)
    end
  end

  defp register_and_block(pid, mref, mode, template, ref, deadline, lease) do
    case safe_call(pid, {:wait, mode, template, ref, lease}) do
      {:ok, tuple} -> {:ok, tuple}
      {:ok, tuple, handle} -> {:ok, tuple, handle}
      :waiting -> block(pid, ref, mref, deadline, lease)
      :down -> :shard_died
    end
  end

  defp block(pid, ref, mref, deadline, lease) do
    receive do
      {__MODULE__, ^ref, payload} -> served(payload, lease)
      {:DOWN, ^mref, :process, _pid, _reason} -> :shard_died
    after
      remaining(deadline) -> cancel(pid, ref, lease)
    end
  end

  # A caller that asked to acknowledge is handed the lease alongside the tuple; everybody
  # else sees the shape they have always seen.
  defp served({tuple, handle}, {:monitor, :ack}), do: {:ok, tuple, handle}
  defp served(tuple, _lease), do: {:ok, tuple}

  defp cancel(pid, ref, lease) do
    _ = safe_call(pid, {:cancel, ref})

    # The shard may have served us in the instant before the cancel arrived, in which case
    # the tuple has already left the space and dropping it would lose it outright. Both
    # messages come from the shard, so ordering guarantees the tuple is already in the
    # mailbox by the time the cancel has been answered.
    receive do
      {__MODULE__, ^ref, payload} -> served(payload, lease)
    after
      0 -> {:error, :timeout}
    end
  end

  defp safe_call(pid, message) do
    GenServer.call(pid, message)
  catch
    :exit, _reason -> :down
  end

  defp deadline(:infinity), do: :infinity
  defp deadline(timeout), do: System.monotonic_time(:millisecond) + timeout

  defp remaining(:infinity), do: :infinity
  defp remaining(deadline), do: max(deadline - System.monotonic_time(:millisecond), 0)

  # -- server -----------------------------------------------------------------

  @impl true
  def init(tag) do
    tab = acquire(tag)

    # The name was registered before init/1 with a nil value; fill in the table now that it
    # exists, so readers can find it without going through this process.
    {_new, _old} = Registry.update_value(@registry, tag, fn _ -> tab end)

    # Reconcile a reclaimed table before reading the counter off it: finishing an
    # interrupted requeue can add a row.
    state = reclaim_leases(%__MODULE__{tag: tag, tab: tab, seq: 1}, Store.recover(tab))

    {:ok, %{state | seq: Store.next_seq(tab)}}
  end

  defp acquire(tag) do
    case TableKeeper.claim(tag) do
      {:ok, tab} -> tab
      :none -> Store.new(@table_label, heir: {Process.whereis(TableKeeper), tag})
    end
  end

  # Re-monitor the holders recorded in a reclaimed table. Monitoring is unconditional: a
  # holder that died while the shard was down produces an immediate :DOWN with :noproc,
  # which requeues through exactly the same path as any other abnormal exit.
  #
  # That window is the one place leasing is at-least-once rather than exactly-once. If a
  # holder finished its work and exited normally while the shard was down, the exit reason
  # is gone with the shard and the tuple is requeued as though the work had failed. Losing
  # it instead would be worse.
  defp reclaim_leases(state, recovered) do
    Enum.reduce(recovered, state, fn {seq, _tuple, ref, pid, mode}, acc ->
      hold(acc, pid, seq, ref, mode)
    end)
  end

  @impl true
  def handle_call({:out, tuple}, _from, %{seq: seq} = state) do
    # Insert first, always — including when a waiter takes the tuple immediately. Serving
    # before inserting would leave a window in which a concurrent caller-side read sees a
    # gap where the tuple never existed, and would put a hole in the sequence accounting.
    :ok = Store.insert(state.tab, seq, tuple)
    state = serve(%{state | seq: seq + 1}, tuple, seq, :out)

    {:reply, {:ok, seq}, state}
  end

  def handle_call({:take, template, lease}, {pid, _tag}, state) do
    case satisfy(state, :in, template, lease, pid) do
      {:ok, tuple, handle, state} -> {:reply, reply(lease, tuple, handle), state}
      :empty -> {:reply, :empty, state}
    end
  end

  def handle_call({:wait, mode, template, ref, lease}, {pid, _tag}, state) do
    # A waiter is only filed if the space cannot satisfy it right now. Registration and
    # `out` are both serialised here, so there is no window between the two in which a
    # matching tuple could sit unnoticed while the caller blocks.
    case satisfy(state, mode, template, lease, pid) do
      {:ok, tuple, handle, state} -> {:reply, reply(lease, tuple, handle), state}
      :empty -> {:reply, :waiting, register(state, mode, template, ref, pid, lease)}
    end
  end

  def handle_call({:cancel, ref}, _from, state) do
    {:reply, :ok, forget_waiter(state, ref)}
  end

  def handle_call(:table, _from, state) do
    {:reply, state.tab, state}
  end

  def handle_call({:watch, ref, template, events, subscriber, max_queue}, {pid, _tag}, state) do
    {:reply, :ok, add_watch(state, ref, template, events, subscriber, max_queue, pid)}
  end

  def handle_call({:unwatch, ref}, _from, state) do
    {:reply, :ok, forget_watch(state, ref)}
  end

  def handle_call({:ack, ref}, _from, state) do
    {:reply, :ok, acknowledge(state, ref)}
  end

  @impl true
  def handle_info({:DOWN, mref, :process, _pid, reason}, state) do
    case Map.get(state.monitors, mref) do
      {:waiter, ref} -> {:noreply, forget_waiter(state, ref)}
      {:watch, ref} -> {:noreply, forget_watch(state, ref)}
      {:lease, ref} -> {:noreply, expire(state, ref, reason)}
      nil -> {:noreply, state}
    end
  end

  # The ETS-TRANSFER that follows a claim from the keeper is only a notification; ownership
  # already moved when give_away/3 was called.
  def handle_info({:"ETS-TRANSFER", _tab, _from, _data}, state), do: {:noreply, state}

  def handle_info(_message, state), do: {:noreply, state}

  # -- leases -----------------------------------------------------------------

  # An acknowledgement is the holder saying the work is done, and in {:monitor, :ack} mode
  # it is the only thing that says so.
  defp acknowledge(state, ref) do
    case Map.get(state.leases, ref) do
      nil -> state
      {seq, _mref, _mode} -> state |> release(seq, ref) |> drop_lease(ref)
    end
  end

  # A holder that dies. In :monitor mode a normal exit means the work was done; every other
  # reason requeues, :shutdown and {:shutdown, _} included, because a supervisor stopping a
  # worker mid-lease is orderly but the work still did not happen.
  #
  # In {:monitor, :ack} mode *no* exit reason discards. The acknowledgement is the signal,
  # and a process that exited without sending one did not finish, however tidily it went.
  defp expire(state, ref, reason) do
    case Map.get(state.leases, ref) do
      nil ->
        state

      {seq, _mref, mode} ->
        state = drop_lease(state, ref)

        if discards?(mode, reason) do
          release(state, seq, ref)
        else
          requeue(state, seq, ref)
        end
    end
  end

  defp discards?(:monitor, :normal), do: true
  defp discards?(_mode, _reason), do: false

  defp release(state, seq, ref) do
    :ok = Store.release(state.tab, seq, ref)
    state
  end

  defp requeue(state, seq, ref) do
    new_seq = state.seq

    case Store.requeue(state.tab, seq, ref, new_seq) do
      {:ok, tuple} ->
        # A requeued tuple is a fresh arrival as far as waiters are concerned — somebody may
        # already be blocked on it.
        serve(%{state | seq: new_seq + 1}, tuple, new_seq, :requeue)

      :error ->
        state
    end
  end

  defp hold(state, pid, seq, ref, mode) do
    mref = Process.monitor(pid)

    %{
      state
      | leases: Map.put(state.leases, ref, {seq, mref, mode}),
        monitors: Map.put(state.monitors, mref, {:lease, ref})
    }
  end

  defp drop_lease(state, ref) do
    case Map.pop(state.leases, ref) do
      {nil, _leases} ->
        state

      {{_seq, mref, _mode}, leases} ->
        Process.demonitor(mref, [:flush])
        %{state | leases: leases, monitors: Map.delete(state.monitors, mref)}
    end
  end

  # -- serving ----------------------------------------------------------------

  defp satisfy(state, :rd, template, _lease, _pid) do
    case Store.read(state.tab, template) do
      {:ok, tuple} -> {:ok, tuple, nil, state}
      :empty -> :empty
    end
  end

  defp satisfy(state, :in, template, false, _pid) do
    case Store.take(state.tab, template) do
      {:ok, tuple} -> {:ok, tuple, nil, announce(state, :in, tuple)}
      :empty -> :empty
    end
  end

  defp satisfy(state, :in, template, mode, pid) do
    ref = make_ref()

    case Store.lease(state.tab, template, ref, pid, mode) do
      {:ok, tuple, seq} ->
        state = state |> hold(pid, seq, ref, mode) |> announce(:in, tuple)
        {:ok, tuple, {state.tag, ref}, state}

      :empty ->
        :empty
    end
  end

  defp serve(state, tuple, seq, event) do
    # Watchers are observational, like readers, so they hear about the tuple before anything
    # can consume it — and because insert comes before serve, what they are told about is
    # something that genuinely existed in the space.
    state = announce(state, event, tuple)

    case Map.get(state.waiters, Template.key(tuple)) do
      nil ->
        state

      bucket ->
        # Reversed into arrival order. The key narrowed the candidates; it does not decide
        # the match, so every template in the bucket is still evaluated.
        {readers, takers} =
          bucket
          |> :lists.reverse()
          |> Enum.filter(&Template.matches?(&1.template, tuple))
          |> Enum.split_with(&(&1.mode == :rd))

        # Every matching reader is woken first...
        state = Enum.reduce(readers, state, &deliver(&2, &1, tuple, nil))

        # ...and only then does one taker consume the tuple. The other order would delete it
        # out from under readers that legitimately matched, leaving them blocked forever on
        # a tuple that has already come and gone.
        case takers do
          [taker | _rest] ->
            {state, handle} = consume(state, taker, seq, tuple)

            state
            |> announce(:in, tuple)
            |> deliver(taker, tuple, handle)

          [] ->
            state
        end
    end
  end

  defp consume(state, %{lease: false}, seq, _tuple) do
    :ok = Store.delete(state.tab, seq)
    {state, nil}
  end

  defp consume(state, %{lease: mode, pid: pid}, seq, tuple) do
    # The row is already in the table, so marking it leased is one insert under the key it
    # already has — the same atomic transition the immediate path gets.
    ref = make_ref()
    :ok = Store.lease_row(state.tab, seq, tuple, ref, pid, mode)
    {hold(state, pid, seq, ref, mode), {state.tag, ref}}
  end

  defp deliver(state, waiter, tuple, handle) do
    send(waiter.pid, {__MODULE__, waiter.ref, payload(waiter.lease, tuple, handle)})
    forget_waiter(state, waiter.ref)
  end

  # Only a caller that asked to acknowledge gets the lease handle back, so the shape of a
  # plain read is unchanged for everybody else.
  defp payload({:monitor, :ack}, tuple, handle), do: {tuple, handle}
  defp payload(_lease, tuple, _handle), do: tuple

  defp reply({:monitor, :ack}, tuple, handle), do: {:ok, tuple, handle}
  defp reply(_lease, tuple, _handle), do: {:ok, tuple}

  # -- watchers ---------------------------------------------------------------

  # Watchers are filed by key, exactly like waiters, so a tuple only has to be offered to
  # the bucket it belongs to. Order within the bucket does not matter — every matching
  # watcher is told and none of them consume — so the list stays newest-first.
  defp announce(state, event, tuple) do
    case Map.get(state.watches, Template.key(tuple)) do
      nil ->
        state

      bucket ->
        bucket
        |> Enum.filter(&(event in &1.events and Template.matches?(&1.template, tuple)))
        |> Enum.reduce(state, &notify(&2, &1, event, tuple))
    end
  end

  # A watcher is pushed at, never asked, so nothing about its own pace reaches the shard. An
  # unbounded send to a subscriber slower than the space is an unbounded mailbox that the
  # shard can neither see nor recover from, so the queue is measured first and events past
  # the threshold are dropped. Lossy on purpose, and reported: a debug console that quietly
  # takes the node down with it is worse than one that misses events and says so.
  defp notify(state, watcher, event, tuple) do
    case Process.info(watcher.subscriber, :message_queue_len) do
      {:message_queue_len, len} when len <= watcher.max_queue ->
        send(watcher.subscriber, {__MODULE__, :watch, watcher.ref, event, tuple})
        state

      {:message_queue_len, len} ->
        :telemetry.execute(
          [:tuplex, :watch, :dropped],
          %{count: 1, message_queue_len: len},
          %{tag: state.tag, ref: watcher.ref, event: event, template: watcher.template}
        )

        state

      nil ->
        # The subscriber is gone. Its Tuplex.Watch will unregister shortly; there is nothing
        # to gain from sending into the void until it does.
        state
    end
  end

  defp add_watch(state, ref, template, events, subscriber, max_queue, registrant) do
    key = Template.key(template)
    mref = Process.monitor(registrant)

    watcher = %{
      ref: ref,
      template: template,
      events: events,
      subscriber: subscriber,
      max_queue: max_queue
    }

    %{
      state
      | watches: Map.update(state.watches, key, [watcher], &[watcher | &1]),
        watch_index: Map.put(state.watch_index, ref, {key, mref}),
        monitors: Map.put(state.monitors, mref, {:watch, ref})
    }
  end

  defp forget_watch(state, ref) do
    case Map.pop(state.watch_index, ref) do
      {nil, _index} ->
        state

      {{key, mref}, index} ->
        Process.demonitor(mref, [:flush])

        %{
          state
          | watches: drop_from_bucket(state.watches, key, ref),
            watch_index: index,
            monitors: Map.delete(state.monitors, mref)
        }
    end
  end

  # -- the waiter index -------------------------------------------------------

  defp register(state, mode, template, ref, pid, lease) do
    key = Template.key(template)
    mref = Process.monitor(pid)
    waiter = %{ref: ref, pid: pid, mode: mode, template: template, lease: lease}

    %{
      state
      | # Newest-first, because prepending is O(1); serve/4 reverses it so waiters are
        # offered in arrival order rather than in whatever order the cons cell produced.
        waiters: Map.update(state.waiters, key, [waiter], &[waiter | &1]),
        waiter_index: Map.put(state.waiter_index, ref, {key, mref}),
        monitors: Map.put(state.monitors, mref, {:waiter, ref})
    }
  end

  defp forget_waiter(state, ref) do
    case Map.pop(state.waiter_index, ref) do
      {nil, _index} ->
        state

      {{key, mref}, index} ->
        Process.demonitor(mref, [:flush])

        %{
          state
          | waiters: drop_from_bucket(state.waiters, key, ref),
            waiter_index: index,
            monitors: Map.delete(state.monitors, mref)
        }
    end
  end

  defp drop_from_bucket(buckets, key, ref) do
    case Enum.reject(Map.get(buckets, key, []), &(&1.ref == ref)) do
      [] -> Map.delete(buckets, key)
      bucket -> Map.put(buckets, key, bucket)
    end
  end

  # -- internals --------------------------------------------------------------

  defp via(tag), do: {:via, Registry, {@registry, tag}}

  # How long a call will keep trying to find a live shard for its tag.
  @resolve_timeout 5_000

  # A shard can die between being resolved and being called, and the registry's cleanup is
  # asynchronous, so for a moment the tag still names the corpse. Now that the table outlives
  # the shard, the right answer is to wait for the replacement rather than to fail.
  #
  # Only a `:noproc` exit is retried. It is the one reason that proves the message was never
  # delivered; any other exit could mean the call was handled and the shard died afterwards,
  # and retrying that would write a tuple twice.
  defp call_shard(tag, message) do
    call_shard(tag, message, System.monotonic_time(:millisecond) + @resolve_timeout)
  end

  defp call_shard(tag, message, deadline) do
    with {:ok, pid} <- ensure(tag) do
      try do
        GenServer.call(pid, message)
      catch
        :exit, {:noproc, _details} -> again(tag, message, deadline, &call_shard/3)
      end
    end
  end

  # As above, but for calls that must not start a shard: with no shard there is nothing to
  # take, unwatch, or acknowledge, and `default` is the honest answer.
  defp call_existing(tag, message, default) do
    call_existing(tag, message, default, System.monotonic_time(:millisecond) + @resolve_timeout)
  end

  defp call_existing(tag, message, default, deadline) do
    case lookup(tag) do
      {:ok, pid, _tab} ->
        try do
          GenServer.call(pid, message)
        catch
          :exit, {:noproc, _details} ->
            again(tag, message, deadline, &call_existing(&1, &2, default, &3))
        end

      :error ->
        default
    end
  end

  defp again(tag, message, deadline, retry) do
    if System.monotonic_time(:millisecond) < deadline do
      Process.sleep(2)
      retry.(tag, message, deadline)
    else
      exit({:noproc, {__MODULE__, :call, [tag, message]}})
    end
  end

  # One retry, never a loop. Which of the three outcomes applies turns on what the registry
  # says the second time.
  defp reread(tag, stale, default, fun) do
    case lookup(tag) do
      # A replacement shard is registered with a table of its own. Retry against it, and let
      # a second failure propagate.
      {:ok, _pid, fresh} when fresh != stale ->
        fun.(fresh)

      # The registry still names the table that just failed. If the shard is gone, the
      # registry has simply not processed the exit yet, and the reference cannot be reached
      # from here, so an empty space is the honest answer and retrying would only raise
      # again. If the shard is alive, it is registered against a table it does not have,
      # which is a bug worth surfacing rather than swallowing.
      {:ok, pid, _same} ->
        if Process.alive?(pid), do: fun.(stale), else: default

      :error ->
        default
    end
  end

  defp in_table(template, default, fun) do
    {tag, _arity} = Template.key(template)

    case lookup(tag) do
      {:ok, _pid, tab} -> attempt(tag, tab, default, fun)
      :error -> default
    end
  end
end
