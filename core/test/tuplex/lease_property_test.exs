defmodule Tuplex.LeasePropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Tuplex.Shard
  alias Tuplex.Store

  # The library's central claim, under the condition that actually threatens it.
  #
  # Every tuple written is consumed exactly once, however many takers are killed while
  # holding it. Each round hands tuples to a batch of leaseholders and then kills or retires
  # each of them; a killed holder's tuple must come back, a retired holder's must not. What
  # finally comes out of the space has to be the multiset that went in, with no tuple
  # consumed twice and none lost.
  #
  # The kill lands while the holder is parked between its take returning and its exit,
  # which is precisely the window the lease exists to cover.
  property "every tuple is consumed exactly once, however takers die" do
    check all(
            count <- integer(1..4),
            rounds <- list_of(list_of(boolean(), min_length: 1, max_length: 4), max_length: 3),
            max_runs: runs()
          ) do
      tag = unique_tag()
      {:ok, _shard} = Shard.ensure(tag)
      tab = table(tag)

      tuples = for n <- 1..count, do: {tag, n}
      Enum.each(tuples, &Tuplex.out/1)

      consumed = Enum.flat_map(rounds, &run_round(tag, tab, &1)) ++ drain(tag)

      assert Enum.sort(consumed) == Enum.sort(tuples),
             "a tuple was consumed twice or lost entirely"

      assert Shard.read_all({tag, :_}) == []
      assert Store.leased(tab) == []
      assert Store.size(tab) == 0
    end
  end

  # Kept modest so the suite stays fast; TUPLEX_PROP_RUNS cranks it up for a real soak.
  defp runs, do: String.to_integer(System.get_env("TUPLEX_PROP_RUNS", "30"))

  # One round: hand tuples to a batch of holders, then retire or kill each. Returns the
  # tuples whose holders retired, which are the ones genuinely consumed.
  defp run_round(tag, tab, plan) do
    holders = for _ <- plan, do: spawn_holder(tag)
    taken = Enum.map(holders, &await_take/1)

    consumed =
      [holders, taken, plan]
      |> Enum.zip()
      |> Enum.flat_map(fn
        {holder, {:ok, tuple}, true} ->
          finish(holder, :normal)
          [tuple]

        {holder, {:ok, _tuple}, false} ->
          finish(holder, :killed)
          []

        {holder, :empty, _complete?} ->
          finish(holder, :normal)
          []
      end)

    # Every lease settled means every release and requeue has been applied, so the free rows
    # are stable before the next round looks at them.
    assert wait_until(fn -> Store.leased(tab) == [] end), "leases did not settle"

    consumed
  end

  # A holder takes without blocking — every tuple is already in the space, so a holder
  # either gets one at once or gets nothing — then parks holding the lease.
  defp spawn_holder(tag) do
    parent = self()

    spawn(fn ->
      send(parent, {:took, self(), Tuplex.inp({tag, :_}, lease: :monitor)})

      receive do
        :retire -> exit(:normal)
      end
    end)
  end

  defp await_take(holder) do
    receive do
      {:took, ^holder, result} -> result
    after
      1_000 -> flunk("holder #{inspect(holder)} never reported")
    end
  end

  defp finish(holder, how) do
    ref = Process.monitor(holder)

    case how do
      :normal -> send(holder, :retire)
      :killed -> Process.exit(holder, :kill)
    end

    receive do
      {:DOWN, ^ref, :process, ^holder, _reason} -> :ok
    after
      1_000 -> flunk("holder #{inspect(holder)} did not exit")
    end
  end

  defp drain(tag, acc \\ []) do
    case Tuplex.inp({tag, :_}) do
      {:ok, tuple} -> drain(tag, [tuple | acc])
      :empty -> acc
    end
  end

  defp table(tag) do
    {:ok, _pid, tab} = Shard.lookup(tag)
    tab
  end

  defp unique_tag do
    String.to_atom("tuplex_leaseprop_tag_#{System.unique_integer([:positive])}")
  end

  defp wait_until(fun, timeout \\ 1_000) do
    wait_until(fun, System.monotonic_time(:millisecond) + timeout, timeout)
  end

  defp wait_until(fun, deadline, timeout) do
    cond do
      fun.() ->
        true

      System.monotonic_time(:millisecond) >= deadline ->
        false

      true ->
        Process.sleep(5)
        wait_until(fun, deadline, timeout)
    end
  end
end
