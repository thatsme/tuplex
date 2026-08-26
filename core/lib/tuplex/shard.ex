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
  tag's table died with it and an empty space is the honest answer, while a live shard
  registered against a table it does not have is a bug and is left to raise.

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
  alias Tuplex.Template

  @registry Tuplex.Registry
  @supervisor Tuplex.ShardSupervisor

  # The table is not :named_table, so this label is only for identification in :ets.i/0 and
  # crash dumps. A constant keeps shard creation from minting a fresh atom per tag.
  @table_label :tuplex_shard

  defstruct [:tag, :tab, :seq, waiters: %{}, index: %{}, monitors: %{}]

  @typedoc "The tag a shard is responsible for."
  @type tag :: atom()

  @typedoc "Whether a waiter consumes the tuple it is given."
  @type mode :: :in | :rd

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

    with {:ok, pid} <- ensure(tag) do
      GenServer.call(pid, {:out, tuple})
    end
  end

  @doc """
  Removes and returns the oldest tuple matching `template`, via its tag's shard.

  Returns `:empty` when nothing matches, and does not start a shard: a tag with no shard
  has nothing to take.
  """
  @spec take(Template.template()) :: {:ok, Template.t()} | :empty
  def take(template) do
    {tag, _arity} = Template.key(template)

    case lookup(tag) do
      {:ok, pid, _tab} -> GenServer.call(pid, {:take, template})
      :error -> :empty
    end
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
  @spec wait(mode(), Template.template(), timeout()) ::
          {:ok, Template.t()} | {:error, :timeout}
  def wait(mode, template, timeout) do
    wait_until(mode, template, deadline(timeout))
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

  defp wait_until(mode, template, deadline) do
    {tag, _arity} = Template.key(template)

    with {:ok, pid} <- ensure(tag) do
      ref = make_ref()
      mref = Process.monitor(pid)
      outcome = register_and_block(pid, mref, mode, template, ref, deadline)
      Process.demonitor(mref, [:flush])

      case outcome do
        # The shard died holding our registration. Blocking means "until a matching tuple
        # arrives", and a crash does not change that contract, so file with its replacement
        # rather than leaving the caller hung or lying about a timeout.
        :shard_died -> retry(mode, template, deadline)
        result -> result
      end
    end
  end

  defp retry(mode, template, deadline) do
    case remaining(deadline) do
      0 -> {:error, :timeout}
      _ -> wait_until(mode, template, deadline)
    end
  end

  defp register_and_block(pid, mref, mode, template, ref, deadline) do
    case safe_call(pid, {:wait, mode, template, ref}) do
      {:ok, tuple} -> {:ok, tuple}
      :waiting -> block(pid, ref, mref, deadline)
      :down -> :shard_died
    end
  end

  defp block(pid, ref, mref, deadline) do
    receive do
      {__MODULE__, ^ref, tuple} -> {:ok, tuple}
      {:DOWN, ^mref, :process, _pid, _reason} -> :shard_died
    after
      remaining(deadline) -> cancel(pid, ref)
    end
  end

  defp cancel(pid, ref) do
    _ = safe_call(pid, {:cancel, ref})

    # The shard may have served us in the instant before the cancel arrived, in which case
    # the tuple has already left the space and dropping it would lose it outright. Both
    # messages come from the shard, so ordering guarantees the tuple is already in the
    # mailbox by the time the cancel has been answered.
    receive do
      {__MODULE__, ^ref, tuple} -> {:ok, tuple}
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
    tab = Store.new(@table_label)

    # The name was registered before init/1 with a nil value; fill in the table now that it
    # exists, so readers can find it without going through this process.
    {_new, _old} = Registry.update_value(@registry, tag, fn _ -> tab end)

    {:ok, %__MODULE__{tag: tag, tab: tab, seq: Store.next_seq(tab)}}
  end

  @impl true
  def handle_call({:out, tuple}, _from, %{seq: seq} = state) do
    # Insert first, always — including when a waiter takes the tuple immediately. Serving
    # before inserting would leave a window in which a concurrent caller-side read sees a
    # gap where the tuple never existed, and would put a hole in the sequence accounting.
    :ok = Store.insert(state.tab, seq, tuple)
    state = serve(%{state | seq: seq + 1}, tuple, seq)

    {:reply, {:ok, seq}, state}
  end

  def handle_call({:take, template}, _from, state) do
    {:reply, Store.take(state.tab, template), state}
  end

  def handle_call({:wait, mode, template, ref}, {pid, _tag}, state) do
    # A waiter is only filed if the space cannot satisfy it right now. Registration and
    # `out` are both serialised here, so there is no window between the two in which a
    # matching tuple could sit unnoticed while the caller blocks.
    case satisfy(state, mode, template) do
      {:ok, tuple} -> {:reply, {:ok, tuple}, state}
      :empty -> {:reply, :waiting, register(state, mode, template, ref, pid)}
    end
  end

  def handle_call({:cancel, ref}, _from, state) do
    {:reply, :ok, forget(state, ref)}
  end

  def handle_call(:table, _from, state) do
    {:reply, state.tab, state}
  end

  @impl true
  def handle_info({:DOWN, mref, :process, _pid, _reason}, state) do
    case Map.fetch(state.monitors, mref) do
      {:ok, ref} -> {:noreply, forget(state, ref)}
      :error -> {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  # -- serving ----------------------------------------------------------------

  defp satisfy(state, :in, template), do: Store.take(state.tab, template)
  defp satisfy(state, :rd, template), do: Store.read(state.tab, template)

  defp serve(state, tuple, seq) do
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
        state = Enum.reduce(readers, state, &deliver(&2, &1, tuple))

        # ...and only then does one taker consume the tuple. The other order would delete it
        # out from under readers that legitimately matched, leaving them blocked forever on
        # a tuple that has already come and gone.
        case takers do
          [taker | _rest] ->
            :ok = Store.delete(state.tab, seq)
            deliver(state, taker, tuple)

          [] ->
            state
        end
    end
  end

  defp deliver(state, waiter, tuple) do
    send(waiter.pid, {__MODULE__, waiter.ref, tuple})
    forget(state, waiter.ref)
  end

  # -- the waiter index -------------------------------------------------------

  defp register(state, mode, template, ref, pid) do
    key = Template.key(template)
    mref = Process.monitor(pid)
    waiter = %{ref: ref, pid: pid, mode: mode, template: template, monitor: mref}

    %{
      state
      | # Newest-first, because prepending is O(1); serve/3 reverses it so waiters are
        # offered in arrival order rather than in whatever order the cons cell produced.
        waiters: Map.update(state.waiters, key, [waiter], &[waiter | &1]),
        index: Map.put(state.index, ref, {key, mref}),
        monitors: Map.put(state.monitors, mref, ref)
    }
  end

  defp forget(state, ref) do
    case Map.pop(state.index, ref) do
      {nil, _index} ->
        state

      {{key, mref}, index} ->
        Process.demonitor(mref, [:flush])

        %{
          state
          | waiters: drop_from_bucket(state.waiters, key, ref),
            index: index,
            monitors: Map.delete(state.monitors, mref)
        }
    end
  end

  defp drop_from_bucket(waiters, key, ref) do
    case Enum.reject(Map.get(waiters, key, []), &(&1.ref == ref)) do
      [] -> Map.delete(waiters, key)
      bucket -> Map.put(waiters, key, bucket)
    end
  end

  # -- internals --------------------------------------------------------------

  defp via(tag), do: {:via, Registry, {@registry, tag}}

  # One retry, never a loop. Which of the three outcomes applies turns on what the registry
  # says the second time.
  defp reread(tag, stale, default, fun) do
    case lookup(tag) do
      # A replacement shard is registered with a table of its own. Retry against it, and let
      # a second failure propagate.
      {:ok, _pid, fresh} when fresh != stale ->
        fun.(fresh)

      # The registry still names the table that just failed. If the shard is gone, the
      # registry has simply not processed the exit yet — the tag's table died with it, so
      # an empty space is the honest answer and retrying would only raise again. If the
      # shard is alive, it is registered against a table it does not have, which is a bug
      # worth surfacing rather than swallowing.
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
