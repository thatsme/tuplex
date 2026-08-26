defmodule Tuplex.Store do
  @moduledoc """
  The tuple table for one shard, and the only module in Tuplex that calls `:ets`.

  `Store` is process-free. It holds no state beyond the table it is handed, spawns nothing,
  and makes no decision about *when* to read or write — that is `Tuplex.Shard`'s job. What
  it owns is the storage form, which is where the sharp edges are.

  ## Storage form

  An `:ordered_set` keyed by a monotonically increasing sequence number:

      {seq, tuple}

  The sequence number comes from the calling shard's counter, so `Store` stays
  deterministic and testable without a process behind it.

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

  Tables are `:public` so that ownership can move to `Tuplex.TableKeeper` while shards
  still write. `Store` assumes the shard owning a table is its only writer: `take/2` reads
  a row and then deletes it in two steps, which is only atomic because the shard serialises
  its own calls. Do not call `take/2` on one table from two processes.
  """

  alias Tuplex.Template

  @typedoc "An ETS table identifier."
  @type tab :: :ets.table()

  @typedoc "A shard-assigned sequence number. Ordering is the shard's to guarantee."
  @type seq :: integer()

  @doc """
  Creates an empty table.

  `:public` so that `Tuplex.TableKeeper` can hold it while shards write to it.
  """
  @spec new(atom()) :: tab()
  def new(name \\ :tuplex_store) when is_atom(name) do
    :ets.new(name, [:ordered_set, :public])
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
  Returns the number of tuples in the table.
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
    [{{:_, head}, guards, [:"$_"]}]
  end
end
