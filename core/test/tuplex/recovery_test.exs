defmodule Tuplex.RecoveryTest do
  use ExUnit.Case, async: true

  alias Tuplex.Shard
  alias Tuplex.Store

  # Store.recover/1 works against the table alone, with no shard behind it, which is the
  # whole point: the table *is* the record, so there is no second structure for it to
  # disagree with.
  describe "Store.recover/1" do
    setup do
      tab = Store.new()
      # No cleanup needed: the table is owned by this test process and dies with it. An
      # on_exit callback runs after that process is gone, so it would only race the teardown.
      {:ok, tab: tab}
    end

    test "reports leased rows", %{tab: tab} do
      ref = make_ref()
      Store.insert(tab, 1, {:job, :free})
      :ok = Store.lease_row(tab, 2, {:job, :held}, ref, self())

      assert [{2, {:job, :held}, ^ref, held_by}] = Store.recover(tab)
      assert held_by == self()
    end

    test "leaves free rows alone", %{tab: tab} do
      Store.insert(tab, 1, {:job, 1})
      assert [] = Store.recover(tab)
      assert [{:job, 1}] = Store.read_all(tab, {:job, :_})
    end

    test "finishes a requeue that had not published the new row", %{tab: tab} do
      :ets.insert(tab, {1, {:job, 1}, {:requeueing, 9}})

      assert [] = Store.recover(tab)
      assert [{:job, 1}] = Store.read_all(tab, {:job, :_})
      assert [{9, {:job, 1}}] = Store.to_list(tab)
    end

    test "finishes a requeue that had published but not retired", %{tab: tab} do
      :ets.insert(tab, {1, {:job, 1}, {:requeueing, 9}})
      :ets.insert(tab, {9, {:job, 1}})

      # Idempotent: the tuple must end up present exactly once, not twice.
      assert [] = Store.recover(tab)
      assert [{:job, 1}] = Store.read_all(tab, {:job, :_})
      assert [{9, {:job, 1}}] = Store.to_list(tab)
    end

    test "is safe to run twice", %{tab: tab} do
      :ets.insert(tab, {1, {:job, 1}, {:requeueing, 9}})

      Store.recover(tab)
      Store.recover(tab)

      assert [{:job, 1}] = Store.read_all(tab, {:job, :_})
    end

    test "runs before next_seq/1 sees the table", %{tab: tab} do
      :ets.insert(tab, {1, {:job, 1}, {:requeueing, 9}})
      Store.recover(tab)

      # The recovered row sits at 9, so the counter has to start past it or the next write
      # would collide.
      assert Store.next_seq(tab) == 10
    end
  end

  describe "surviving a shard crash" do
    setup do
      tag = unique_tag()
      {:ok, pid} = Shard.ensure(tag)
      {:ok, tag: tag, shard: pid}
    end

    test "free tuples outlive the shard that held them", %{tag: tag, shard: shard} do
      Tuplex.out({tag, 1})
      Tuplex.out({tag, 2})

      restart(tag, shard)

      assert [{^tag, 1}, {^tag, 2}] = Shard.read_all({tag, :_})
    end

    test "the sequence counter picks up where the table left off", %{tag: tag, shard: shard} do
      Tuplex.out({tag, 1})
      Tuplex.out({tag, 2})

      restart(tag, shard)

      assert {:ok, 3} = Shard.out({tag, 3})
    end

    test "a lease whose holder is still alive is picked back up", %{tag: tag, shard: shard} do
      Tuplex.out({tag, 1})
      holder = spawn_holder(tag)
      assert_receive {:took, ^holder, {:ok, {^tag, 1}}}, 1_000

      restart(tag, shard)

      # Still held, still invisible, and the new shard is watching the holder again.
      assert [{_seq, {^tag, 1}, _ref, ^holder}] = Store.leased(table(tag))
      assert :empty = Tuplex.rdp({tag, :_})

      send(holder, {:finish, :boom})

      assert wait_until(fn -> Tuplex.rdp({tag, :_}) == {:ok, {tag, 1}} end),
             "the re-monitored lease did not requeue"
    end

    test "a lease whose holder died in the meantime is requeued", %{tag: tag, shard: shard} do
      Tuplex.out({tag, 1})
      holder = spawn_holder(tag)
      assert_receive {:took, ^holder, {:ok, {^tag, 1}}}, 1_000

      # Kill the shard first, so nothing is watching when the holder goes.
      ref = Process.monitor(shard)
      Process.exit(shard, :kill)
      assert_receive {:DOWN, ^ref, :process, ^shard, :killed}

      holder_ref = Process.monitor(holder)
      Process.exit(holder, :kill)
      assert_receive {:DOWN, ^holder_ref, :process, ^holder, :killed}

      assert wait_until(fn -> match?({:ok, _, _}, Shard.lookup(tag)) end)

      # Monitoring a dead holder produces an immediate :noproc, which requeues down the
      # ordinary path.
      assert wait_until(fn -> Tuplex.rdp({tag, :_}) == {:ok, {tag, 1}} end),
             "a lease orphaned by the crash was never requeued"

      assert Store.leased(table(tag)) == []
    end
  end

  # -- helpers ----------------------------------------------------------------

  defp restart(tag, shard) do
    ref = Process.monitor(shard)
    Process.exit(shard, :kill)
    assert_receive {:DOWN, ^ref, :process, ^shard, :killed}

    assert wait_until(fn ->
             match?({:ok, pid, _tab} when pid != shard, Shard.lookup(tag))
           end),
           "no replacement shard appeared"
  end

  defp spawn_holder(tag) do
    parent = self()

    spawn(fn ->
      send(parent, {:took, self(), Tuplex.in({tag, :_}, lease: true)})

      receive do
        {:finish, reason} -> exit(reason)
      end
    end)
  end

  defp table(tag) do
    {:ok, _pid, tab} = Shard.lookup(tag)
    tab
  end

  defp unique_tag do
    String.to_atom("tuplex_recover_tag_#{System.unique_integer([:positive])}")
  end

  defp wait_until(fun, timeout \\ 2_000) do
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
