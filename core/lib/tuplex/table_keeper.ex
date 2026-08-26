defmodule Tuplex.TableKeeper do
  @moduledoc """
  Holds shard tables between a shard's death and its replacement's start, so a crash costs
  a process rather than a space.

  Every shard table is created with this process as its ETS **heir**. When a shard dies the
  table is handed here instead of being destroyed; when the replacement shard starts it
  claims the table back, and `Tuplex.Store.recover/1` reconciles whatever the crash
  interrupted.

  The keeper deliberately does almost nothing. It holds no tuples of its own, makes no
  decisions about their contents, and never writes to a table it is holding. Tables whose
  heir is a dead process are destroyed with it, so the one job that matters here is staying
  alive — which is easiest to guarantee by having no logic that could fail.

  ## The ordering it depends on

  A restarting shard claims before creating, so a table is only ever recreated if the
  keeper does not have one. That leans on the transfer reaching the keeper before the
  replacement shard's claim does, which it does by a wide margin: ETS hands the table over
  at the instant the owner dies, one message hop away, while the replacement has to wait
  for the supervisor to process the exit, the registry to release the name, and a new
  process to start and call. If it ever did lose that race the cost is a fresh empty table
  and an orphaned one held here, not corruption.
  """

  use GenServer

  @doc false
  def start_link(_opts) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @doc """
  Claims the table held for `tag`, transferring ownership to the calling process.

  Returns `{:ok, tab}`, or `:none` if no table is being held — in which case the caller
  should create one.
  """
  @spec claim(atom()) :: {:ok, :ets.table()} | :none
  def claim(tag) do
    GenServer.call(__MODULE__, {:claim, tag})
  end

  @impl true
  def init(:ok), do: {:ok, %{}}

  @impl true
  def handle_call({:claim, tag}, {pid, _tag}, tables) do
    case Map.pop(tables, tag) do
      {nil, tables} ->
        {:reply, :none, tables}

      {tab, tables} ->
        # Ownership moves synchronously here; the ETS-TRANSFER message that follows is only
        # a notification, which the shard ignores.
        true = :ets.give_away(tab, pid, tag)
        {:reply, {:ok, tab}, tables}
    end
  end

  @impl true
  def handle_info({:"ETS-TRANSFER", tab, _from, tag}, tables) do
    {:noreply, Map.put(tables, tag, tab)}
  end

  def handle_info(_message, tables), do: {:noreply, tables}
end
