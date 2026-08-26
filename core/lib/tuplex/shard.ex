defmodule Tuplex.Shard do
  @moduledoc """
  One GenServer per tag, owning that tag's table, its sequence counter, and — from step 3
  — its waiters and leases.

  Shards are started on demand: the first `out/2` for a tag resolves the tag to a pid and
  starts a shard if there is none. Resolution goes through `Tuplex.Registry`, whose value
  carries the shard's **table reference** alongside its pid:

      Registry.register(Tuplex.Registry, tag, table_ref)

  That extra field is what makes the read path cheap, and `tags/0` falls out of the same
  registry for free rather than being a feature of its own.

  ## Destructive operations are serialised; reads are not

  Serialising through the shard exists to stop two consumers taking the same tuple. That is
  a property of *destructive* operations only. `read/2` and `read_all/2` mutate nothing and
  ETS reads are atomic per object, so a GenServer round-trip would buy no correctness while
  costing a message hop and head-of-line blocking behind every `out` already in the
  mailbox.

  So non-destructive reads run **in the calling process**: one ETS lookup in the registry
  for the table reference, then one select. No shard involvement, fully parallel across
  schedulers. `out/2` and `take/2` always go through the shard.

  This is not a micro-optimisation. The blackboard layer this library is eventually for is
  `rd`-dominant by nature — many knowledge sources examining the same hypotheses — and a
  read path that serialises on the tag's shard would bottleneck that layer on day one, on a
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

  **Asymmetric freshness.** `take/2` is serialised and exact: what it returns was in the
  space and is now yours. `read/2` is lock-free and returns a snapshot that may already be
  stale by the time the caller sees it — a tuple it found can be taken by someone else
  immediately after. That is the honest description of a lock-free read, and it is the
  same local-exactness caveat that applies to `inp` generally.
  """

  use GenServer

  alias Tuplex.Store
  alias Tuplex.Template

  @registry Tuplex.Registry
  @supervisor Tuplex.ShardSupervisor

  # The table is not :named_table, so this label is only for identification in :ets.i/0 and
  # crash dumps. A constant keeps shard creation from minting a fresh atom per tag.
  @table_label :tuplex_shard

  defstruct [:tag, :tab, :seq]

  @typedoc "The tag a shard is responsible for."
  @type tag :: atom()

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
    :ok = Store.insert(state.tab, seq, tuple)
    {:reply, {:ok, seq}, %{state | seq: seq + 1}}
  end

  def handle_call({:take, template}, _from, state) do
    {:reply, Store.take(state.tab, template), state}
  end

  def handle_call(:table, _from, state) do
    {:reply, state.tab, state}
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
