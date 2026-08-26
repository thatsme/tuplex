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

  ## Importing

  `in/2` keeps its Linda name, which collides with `Kernel.in/2`. Alias rather than import,
  or import with the clash excluded:

      import Tuplex, except: [in: 2]

  `take/2` is an alias for `in/2` if you would rather not deal with the name at all.

  > #### Work in progress {: .warning}
  >
  > v0.1 is still being built. Available now: `out/1`, `in/2`, `rd/2`, `inp/1`, `rdp/1`,
  > `take/2`, `tags/0`. Still to come: `rd_all/1`, `eval/1`, `watch/1`, leases, and
  > telemetry.
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
  Blocks until a tuple matching `template` is available, then removes and returns it.

  Returns `{:ok, tuple}`, or `{:error, :timeout}` if the wait runs out.

  ## Options

    * `:timeout` — milliseconds to wait, or `:infinity` (the default). `0` does not
      register a waiter at all; it behaves exactly like `inp/1`, reporting
      `{:error, :timeout}` where `inp/1` would say `:empty`.

  Exactly one blocked `in/2` receives any given tuple. Every `rd/2` waiting on a template
  that also matches is woken **first**, before this call consumes it, so a reader never
  ends up blocked on a tuple that has already been taken away.

  Waiters are served in the order they arrived.

  ## The name

  `in` is a binary operator in Elixir, so this is defined as `def unquote(:in)`. That is
  invisible at the call site but it does mean `import Tuplex` clashes with `Kernel.in/2` —
  alias the module, import with `except: [in: 2]`, or use `take/2`.

  ## Examples

      Task.async(fn -> Tuplex.in({:job, :_}) end)
      Tuplex.out({:job, 1})
      #=> the blocked task returns {:ok, {:job, 1}}

      Tuplex.in({:nothing, :_}, timeout: 10)
      #=> {:error, :timeout}
  """
  @spec unquote(:in)(Template.template(), keyword()) ::
          {:ok, Template.t()} | {:error, :timeout}
  def unquote(:in)(template, opts \\ []) do
    template = Template.validate!(template)
    blocking(:in, template, timeout!(opts))
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
    blocking(:rd, template, timeout!(opts))
  end

  @doc """
  An alias for `in/2`, for callers who would rather not work around the operator name.
  """
  @spec take(Template.template(), keyword()) :: {:ok, Template.t()} | {:error, :timeout}
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

  # -- internals --------------------------------------------------------------

  # A single funnel per operation, so that step 8 can wrap each one in a telemetry span at
  # one call site instead of threading events through several early returns.
  defp blocking(mode, template, 0) do
    # A zero timeout is the non-blocking probe. Registering a waiter only to sweep it in the
    # same breath would cost two shard calls and a monitor to reach the same answer.
    case probe(mode, template) do
      {:ok, tuple} -> {:ok, tuple}
      :empty -> {:error, :timeout}
    end
  end

  defp blocking(mode, template, timeout), do: Shard.wait(mode, template, timeout)

  defp probe(:in, template), do: Shard.take(template)
  defp probe(:rd, template), do: Shard.read(template)

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
