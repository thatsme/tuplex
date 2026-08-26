defmodule Tuplex.Store do
  @moduledoc """
  > #### Internal {: .warning}
  >
  > Published because its semantics are load-bearing for anyone reasoning about the space —
  > the storage form is where the sharp edges are. It is **not part of the public API and
  > not covered by semantic versioning**: the record layout will change when ephemeral
  > tuples land, and no version bump will be considered breaking for it. Use `Tuplex`.

  The tuple table for one shard, and the only module in Tuplex that calls `:ets`.

  `Store` is process-free. It holds no state beyond the table it is handed, spawns nothing,
  and makes no decision about *when* to read or write — that is the shard's job. What
  it owns is the storage form, which is where the sharp edges are.

  ## Storage form

  An `:ordered_set` keyed by a monotonically increasing sequence number:

      {seq, tuple}                             # free
      {seq, tuple, {:leased, ref, pid, mode}}  # held by a consumer
      {seq, tuple, {:requeueing, new_seq}}     # a requeue caught mid-flight

  The sequence number comes from the calling shard's counter, so `Store` stays
  deterministic and testable without a process behind it.

  A leased tuple is **not** removed from the table, only marked, which makes the
  free-to-leased transition a single atomic `:ets.insert/2` under the same key. See
  `lease/5` for why that matters. The mode travels in the row too, so a shard reclaiming a
  table after a crash knows how each held tuple is meant to be settled. The third element also means a leased row has arity 3,
  so the arity-2 head pattern every read uses cannot match it — leased tuples are invisible
  to `read/2`, `read_all/2` and `take/2` for free.

  Excluding them from `read/2` is a judgement rather than a necessity: a lease is a claim,
  not a deletion, so an argument exists for showing readers what is in flight. But a leased
  tuple will either be consumed or requeued, and surfacing an in-flight claim would make
  `rd` results depend on consumer timing. They stay hidden.

  The obvious alternative, a `:duplicate_bag` keyed by the tuple's tag, does not work, and
  the reason is worth recording because it is invisible until it costs you data. A
  destructive read has to remove **exactly one** of N identical rows — that is the whole
  semaphore idiom, `out` a token three times and `in` it back one at a time. A
  `:duplicate_bag` has no primitive that does this:

    * `:ets.delete_object/2` deletes *every* object equal to the one given, so taking one
      token drains all three at once.
    * `:ets.take/2` takes by key, and in a per-tag shard the key is the tag, so it removes
      the entire space.

  Giving every row a distinct key solves it outright: deleting exactly one row is
  `:ets.delete(tab, seq)`.

  Nothing is lost by moving off `:duplicate_bag`. Within a shard every tuple carries the
  same tag by construction, so a tag-keyed index selects everything and every match is a
  linear scan whatever the table type. Two things are gained. Traversal of an
  `:ordered_set` follows key order, so the first match is the oldest match and reads are
  FIFO — Linda leaves the choice among matching tuples unspecified, and predictable is
  strictly more useful than arbitrary at no cost. And insert and delete become O(log n),
  which is noise next to the O(n) scan they accompany.

  ## Match specs

  Specs are built here, not in `Tuplex.Template`, because the record layout is this
  module's business. `Template.compile/1` returns a head shaped like the template plus any
  equality guards; `Store` nests that head under the sequence position:

      [{{:_, head}, guards, [:"$_"]}]

  The body returns the whole record, so a destructive read can read the `seq` back out of
  the row it just matched. Because `Store` contributes only `:_` and never a numbered
  variable, `Template`'s `:"$1"`, `:"$2"`, … can never collide with anything here.

  Destructive and single reads use `:ets.select/3` with a limit of 1 rather than selecting
  everything and taking the head — early termination is what keeps a read on a shard
  holding real volume from walking the whole table.

  ## Concurrency

  Tables are `:protected` with `read_concurrency: true`: the owning shard is the only
  writer, and any process may read.

  That asymmetry is the point. Serialising through the shard exists to stop two consumers
  taking the same tuple, which is a property of *destructive* operations only. `read/2` and
  `read_all/2` mutate nothing and ETS reads are atomic per object, so routing them through
  a GenServer would buy no correctness while costing a message round-trip and head-of-line
  blocking behind every queued `out`. The shard therefore runs them in the calling
  process; `take/2` and `insert/3` it keeps to itself.

  `take/2` reads a row and then deletes it in two steps, which is atomic only because the
  shard serialises its own calls. Never call `take/2` or `insert/3` on one table from two
  processes.
  """

  alias Tuplex.Template

  @typedoc "An ETS table identifier."
  @type tab :: :ets.table()

  @typedoc "A shard-assigned sequence number. Ordering is the shard's to guarantee."
  @type seq :: integer()

  @doc """
  Creates an empty table owned by the calling process.

  `:protected`, so only the owner writes and every process reads; `read_concurrency: true`
  because the read path is the parallel one.

  ## Options

    * `:heir` — `{pid, data}` to hand the table to if the owner dies, which is how
      the table keeper keeps a shard's tuples alive across a crash.
  """
  @spec new(atom(), keyword()) :: tab()
  def new(name \\ :tuplex_store, opts \\ []) when is_atom(name) do
    heir =
      case Keyword.fetch(opts, :heir) do
        {:ok, {pid, data}} -> [{:heir, pid, data}]
        :error -> []
      end

    :ets.new(name, [:ordered_set, :protected, {:read_concurrency, true} | heir])
  end

  @doc """
  Returns the sequence number a shard should write next.

  Derived from the table rather than started at zero. At v0.1 a crashed shard loses its
  table and zero would do, but once the table keeper hands a reclaimed table back the
  rows in it are already numbered — a counter restarting at zero would collide on the first
  insert and trip `insert/3`'s raise. That is the right failure, but a needless one, and
  deriving the counter here makes the handover a no-op.

  ## Examples

      iex> tab = Tuplex.Store.new()
      iex> Tuplex.Store.next_seq(tab)
      1
      iex> Tuplex.Store.insert(tab, 7, {:job, 1})
      iex> Tuplex.Store.next_seq(tab)
      8
  """
  @spec next_seq(tab()) :: seq()
  def next_seq(tab) do
    case :ets.last(tab) do
      :"$end_of_table" -> 1
      last -> last + 1
    end
  end

  @doc """
  Deletes the table.
  """
  @spec destroy(tab()) :: :ok
  def destroy(tab) do
    true = :ets.delete(tab)
    :ok
  end

  @doc """
  Writes `tuple` under `seq`.

  Raises if `seq` is already present. An `:ordered_set` would otherwise silently overwrite
  the existing row, losing a tuple with no error anywhere — a shard handing out a duplicate
  sequence number is a bug worth failing loudly on.
  """
  @spec insert(tab(), seq(), Template.t()) :: :ok
  def insert(tab, seq, tuple) do
    if :ets.insert_new(tab, {seq, tuple}) do
      :ok
    else
      raise ArgumentError,
            "sequence #{inspect(seq)} is already present in the store; " <>
              "each tuple needs a fresh sequence number"
    end
  end

  @doc """
  Removes and returns the oldest tuple matching `template`.

  Returns `:empty` when nothing matches. `:empty` is an ordinary outcome rather than an
  error: an empty space is a normal state for a tuple space to be in, unlike the timeout a
  blocking read can fail with.

  Removes exactly one row even when the space holds several identical tuples.
  """
  @spec take(tab(), Template.template()) :: {:ok, Template.t()} | :empty
  def take(tab, template) do
    case first(tab, template) do
      {seq, tuple} ->
        true = :ets.delete(tab, seq)
        {:ok, tuple}

      nil ->
        :empty
    end
  end

  @doc """
  Returns the oldest tuple matching `template`, leaving it in place.

  Returns `:empty` when nothing matches.
  """
  @spec read(tab(), Template.template()) :: {:ok, Template.t()} | :empty
  def read(tab, template) do
    case first(tab, template) do
      {_seq, tuple} -> {:ok, tuple}
      nil -> :empty
    end
  end

  @doc """
  Returns every tuple matching `template`, oldest first, leaving them all in place.

  Identical tuples appear once each.
  """
  @spec read_all(tab(), Template.template()) :: [Template.t()]
  def read_all(tab, template) do
    tab
    |> :ets.select(spec(template))
    |> Enum.map(&elem(&1, 1))
  end

  @doc """
  Removes the row written under `seq`, if it is still there.

  This is how a lease releases its tuple: the shard remembers the sequence number it was
  given and hands it back when the leasing process dies. Returns `:ok` whether or not the
  row was still present — a tuple already taken by a reader is not an error.
  """
  @spec delete(tab(), seq()) :: :ok
  def delete(tab, seq) do
    true = :ets.delete(tab, seq)
    :ok
  end

  @doc """
  Leases the oldest free tuple matching `template` to `pid`, without removing it.

  Returns `{:ok, tuple, seq}`, or `:empty` when nothing matches.

  The row is **marked in place** — `{seq, tuple}` becomes `{seq, tuple, {:leased, ref,
  pid}}` — which an `:ordered_set` does in a single `:ets.insert/2` under the same key.
  That atomicity is the whole point of the design.

  The alternative, deleting the row and recording the lease somewhere else, cannot be done
  in one operation, and every ordering of the two writes has a failure mode: record first
  and a crash in between leaves the row present *and* a lease claiming it needs requeuing,
  which is duplicate delivery; delete first and a crash in between loses the tuple with no
  record that it ever existed, which is exactly the silent loss this library promises not
  to do. Marking in place removes the window rather than narrowing it, and leaves the table
  itself as the authoritative record of who holds what.
  """
  @spec lease(tab(), Template.template(), reference(), pid(), term()) ::
          {:ok, Template.t(), seq()} | :empty
  def lease(tab, template, ref, pid, mode) do
    case first(tab, template) do
      {seq, tuple} ->
        :ok = lease_row(tab, seq, tuple, ref, pid, mode)
        {:ok, tuple, seq}

      nil ->
        :empty
    end
  end

  @doc """
  Marks the row already sitting at `seq` as leased to `pid`.

  For the case where the tuple has just been written and handed straight to a waiter: it is
  already in the table, so there is nothing to select.
  """
  @spec lease_row(tab(), seq(), Template.t(), reference(), pid(), term()) :: :ok
  def lease_row(tab, seq, tuple, ref, pid, mode) do
    true = :ets.insert(tab, {seq, tuple, {:leased, ref, pid, mode}})
    :ok
  end

  @doc """
  Discards a leased row: the holder finished with it.

  `ref` must match the lease recorded on the row, so a stale expiry cannot delete a tuple
  that has since been requeued and leased to somebody else. Returns `:ok` either way.
  """
  @spec release(tab(), seq(), reference()) :: {:ok, Template.t()} | :error
  def release(tab, seq, ref) do
    case :ets.lookup(tab, seq) do
      [{^seq, tuple, {:leased, ^ref, _pid, _mode}}] ->
        true = :ets.delete(tab, seq)
        {:ok, tuple}

      _other ->
        :error
    end
  end

  @doc """
  Returns a leased row to the space at `new_seq`, as a free tuple.

  Returns `{:ok, tuple}` so the caller can offer it to waiters, or `:error` if `ref` no
  longer matches the lease on the row — which makes the call idempotent against a repeated
  or stale expiry.

  A fresh sequence number puts the tuple at the **back** of the queue rather than back
  where it was. Reinserting at the original sequence would preserve arrival order, which is
  arguably fairer for job dispatch, but it also means a tuple that crashes whoever takes it
  is handed straight back to the next taker in a tight loop. At the back, the same poison
  tuple starves rather than stalls, which is visible instead of fatal.

  The three writes are ordered so that a crash at any point leaves the table recoverable:
  the intent is recorded first, atomically, then the tuple is published at its new
  position, then the old row retires. `recover/1` finishes whatever was interrupted.
  """
  @spec requeue(tab(), seq(), reference(), seq()) :: {:ok, Template.t()} | :error
  def requeue(tab, seq, ref, new_seq) do
    case :ets.lookup(tab, seq) do
      [{^seq, tuple, {:leased, ^ref, _pid, _mode}}] ->
        true = :ets.insert(tab, {seq, tuple, {:requeueing, new_seq}})
        true = :ets.insert(tab, {new_seq, tuple})
        true = :ets.delete(tab, seq)
        {:ok, tuple}

      _other ->
        :error
    end
  end

  @doc """
  Prepares a reclaimed table for use and reports the leases still recorded in it.

  Finishes any requeue that a crash interrupted — idempotently, by checking whether the
  tuple already reached its new position — and then returns `{seq, tuple, ref, pid}` for
  every row still marked leased.

  This is authoritative precisely because the table *is* the record. There is no second
  structure to reconcile it against and no way for the two to disagree.

  Call it before `next_seq/1`, since finishing a requeue can add a row.
  """
  @spec recover(tab()) :: [{seq(), Template.t(), reference(), pid(), term()}]
  def recover(tab) do
    rows = held(tab)

    for {seq, tuple, {:requeueing, new_seq}} <- rows do
      if :ets.lookup(tab, new_seq) == [], do: :ets.insert(tab, {new_seq, tuple})
      :ets.delete(tab, seq)
    end

    for {seq, tuple, {:leased, ref, pid, mode}} <- rows, do: {seq, tuple, ref, pid, mode}
  end

  @doc """
  Returns `{seq, tuple, ref, pid}` for every currently leased row. For tests and
  introspection.
  """
  @spec leased(tab()) :: [{seq(), Template.t(), reference(), pid(), term()}]
  def leased(tab) do
    for {seq, tuple, {:leased, ref, pid, mode}} <- held(tab), do: {seq, tuple, ref, pid, mode}
  end

  @doc """
  Returns the number of rows in the table, leased ones included.
  """
  @spec size(tab()) :: non_neg_integer()
  def size(tab), do: :ets.info(tab, :size)

  @doc """
  Returns every `{seq, tuple}` row, oldest first. For tests and introspection.
  """
  @spec to_list(tab()) :: [{seq(), Template.t()}]
  def to_list(tab), do: :ets.tab2list(tab)

  # -- internals --------------------------------------------------------------

  # Ordered-set traversal follows key order, so the first row select returns is the one
  # with the lowest sequence number: the oldest match.
  defp first(tab, template) do
    case :ets.select(tab, spec(template), 1) do
      {[record], _continuation} -> record
      :"$end_of_table" -> nil
    end
  end

  defp spec(template) do
    {head, guards} = Template.compile!(template)

    # A two-element head. A leased row is a three-element record, so it cannot match this
    # pattern at all — excluding leases from every read costs nothing, not even a guard,
    # because arity is already part of how ETS matches.
    [{{:_, head}, guards, [:"$_"]}]
  end

  # Every row carrying a third element: leased, or mid-requeue.
  defp held(tab), do: :ets.select(tab, [{{:_, :_, :_}, [], [:"$_"]}])
end
