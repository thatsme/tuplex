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

  | If you need                       | Use              |
  | --------------------------------- | ---------------- |
  | Durable, retried background jobs  | Oban             |
  | Backpressured data pipelines      | Broadway         |
  | Topic fan-out to known subscribers| Phoenix.PubSub   |
  | Named process lookup              | Registry         |

  Tuplex earns its place only where the coordination is genuinely anonymous and
  shape-driven.

  > #### Work in progress {: .warning}
  >
  > v0.1 is still being built. Available now: `out/1`, `inp/1`, `rdp/1`, `tags/0`. The
  > blocking reads `in/2` and `rd/2`, plus `rd_all/1`, `eval/1`, `watch/1` and leases, are
  > not implemented yet.
  """

  alias Tuplex.Shard
  alias Tuplex.Template

  @doc """
  Writes `tuple` into the space.

  The tuple's tag selects its shard, which is started on demand. Raises `ArgumentError` if
  the tuple has no concrete atom tag.

  Identical tuples do not collapse: writing `{:token}` three times means three tuples, and
  three `inp/1` calls to drain them.

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
  """
  @spec inp(Template.template()) :: {:ok, Template.t()} | :empty
  def inp(template) do
    template = Template.validate!(template)
    Shard.take(template)
  end

  @doc """
  Returns the oldest tuple matching `template` without removing it, and without blocking.

  Returns `{:ok, tuple}`, or `:empty` if nothing matches right now.

  Unlike `inp/1`, this runs **in the calling process** — a registry lookup and one ETS
  select, with no shard round-trip — so reads are fully parallel and never queue behind
  pending writes.

  The trade is freshness, and it is worth stating plainly rather than papering over: what
  you get back is a snapshot. Another process may take the tuple you just read before you
  do anything with it. If you need the tuple to be *yours*, use `inp/1`.

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
    Shard.read(template)
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
end
