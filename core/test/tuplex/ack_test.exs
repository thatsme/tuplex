defmodule Tuplex.AckTest do
  use ExUnit.Case, async: true

  alias Tuplex.Shard
  alias Tuplex.Store

  setup do
    tag = unique_tag()
    {:ok, pid} = Shard.ensure(tag)
    {:ok, tag: tag, shard: pid}
  end

  describe "the shape of the return" do
    test "{:monitor, :ack} hands back a handle", %{tag: tag} do
      Tuplex.out({tag, 1})
      assert {:ok, {^tag, 1}, handle} = Tuplex.inp({tag, :_}, lease: {:monitor, :ack})
      assert is_tuple(handle)
    end

    test ":monitor and no lease keep the two-element shape", %{tag: tag} do
      Tuplex.out({tag, 1})
      Tuplex.out({tag, 2})

      assert {:ok, {^tag, 1}} = Tuplex.inp({tag, :_}, lease: :monitor)
      assert {:ok, {^tag, 2}} = Tuplex.inp({tag, :_})
    end

    test "a blocked in/2 in ack mode gets the handle too", %{tag: tag, shard: shard} do
      task = Task.async(fn -> Tuplex.in({tag, :_}, lease: {:monitor, :ack}) end)
      assert wait_until(fn -> waiter_count(shard) == 1 end)

      Tuplex.out({tag, 1})

      assert {:ok, {^tag, 1}, handle} = Task.await(task)
      assert is_tuple(handle)
    end
  end

  # The whole reason ack mode exists. In :monitor mode a worker that takes many tuples over
  # its life holds a live lease for every one of them, and a crash at the 501st requeues all
  # 500 — including 499 whose work was finished. That is not a papercut, it is a silent
  # violation of the exactly-once claim.
  describe "a long-lived worker" do
    test "holds at most one lease at a time when it acknowledges", %{tag: tag} do
      for n <- 1..5, do: Tuplex.out({tag, n})
      tab = table(tag)
      parent = self()

      worker =
        spawn(fn ->
          Enum.each(1..5, fn _ ->
            {:ok, tuple, handle} = Tuplex.inp({tag, :_}, lease: {:monitor, :ack})
            send(parent, {:held, tuple, length(Store.leased(tab))})
            :ok = Tuplex.ack(handle)
          end)

          send(parent, :done)
          Process.sleep(:infinity)
        end)

      for n <- 1..5 do
        assert_receive {:held, {^tag, ^n}, held}, 1_000
        assert held == 1, "worker was holding #{held} leases at once"
      end

      assert_receive :done, 1_000
      assert Store.leased(tab) == []
      assert Store.size(tab) == 0

      # Crashing now costs nothing: everything it did was acknowledged.
      Process.exit(worker, :kill)
      assert wait_until(fn -> Tuplex.rd_all({tag, :_}) == [] end)
    end

    test "in :monitor mode the same worker would hold all five", %{tag: tag} do
      for n <- 1..5, do: Tuplex.out({tag, n})
      tab = table(tag)
      parent = self()

      worker =
        spawn(fn ->
          Enum.each(1..5, fn _ ->
            {:ok, _tuple} = Tuplex.inp({tag, :_}, lease: :monitor)
          end)

          send(parent, :done)
          Process.sleep(:infinity)
        end)

      assert_receive :done, 1_000
      assert length(Store.leased(tab)) == 5

      # ...and a crash requeues all of them, finished work included. Documented behaviour
      # rather than a bug, which is why :monitor is for callers whose lifetime *is* the
      # unit of work.
      Process.exit(worker, :kill)
      assert wait_until(fn -> length(Tuplex.rd_all({tag, :_})) == 5 end)
    end
  end

  describe "settling an ack lease" do
    test "ack discards the tuple", %{tag: tag} do
      Tuplex.out({tag, 1})
      {:ok, _tuple, handle} = Tuplex.inp({tag, :_}, lease: {:monitor, :ack})

      assert :ok = Tuplex.ack(handle)
      assert Store.leased(table(tag)) == []
      assert Store.size(table(tag)) == 0
    end

    test "a normal exit without an ack still requeues", %{tag: tag} do
      Tuplex.out({tag, 1})
      holder = spawn_holder(tag)
      assert_receive {:took, ^holder, {:ok, {^tag, 1}, _handle}}, 1_000

      send(holder, {:finish, :normal})

      # The distinguishing case: :monitor would treat this as work done and discard.
      assert wait_until(fn -> Tuplex.rdp({tag, :_}) == {:ok, {tag, 1}} end),
             "a normal exit without an acknowledgement should not count as finished"
    end

    test "a crash requeues, as ever", %{tag: tag} do
      Tuplex.out({tag, 1})
      holder = spawn_holder(tag)
      assert_receive {:took, ^holder, {:ok, {^tag, 1}, _handle}}, 1_000

      Process.exit(holder, :kill)
      assert wait_until(fn -> Tuplex.rdp({tag, :_}) == {:ok, {tag, 1}} end)
    end

    test "acknowledging twice is harmless", %{tag: tag} do
      Tuplex.out({tag, 1})
      {:ok, _tuple, handle} = Tuplex.inp({tag, :_}, lease: {:monitor, :ack})

      assert :ok = Tuplex.ack(handle)
      assert :ok = Tuplex.ack(handle)
    end

    test "acknowledging an expired lease is harmless", %{tag: tag} do
      Tuplex.out({tag, 1})
      holder = spawn_holder(tag)
      assert_receive {:took, ^holder, {:ok, _tuple, handle}}, 1_000

      Process.exit(holder, :kill)
      assert wait_until(fn -> Tuplex.rdp({tag, :_}) == {:ok, {tag, 1}} end)

      # The tuple has been requeued and may already belong to someone else, so a late
      # acknowledgement must not delete it.
      assert :ok = Tuplex.ack(handle)
      assert {:ok, {^tag, 1}} = Tuplex.rdp({tag, :_})
    end

    test "the lease mode survives a shard crash", %{tag: tag, shard: shard} do
      Tuplex.out({tag, 1})
      holder = spawn_holder(tag)
      assert_receive {:took, ^holder, {:ok, {^tag, 1}, _handle}}, 1_000

      down = Process.monitor(shard)
      Process.exit(shard, :kill)
      assert_receive {:DOWN, ^down, :process, ^shard, :killed}

      assert wait_until(fn ->
               match?({:ok, pid, _tab} when pid != shard, Shard.lookup(tag))
             end)

      # The mode travels in the row, so the replacement knows this lease needs an
      # acknowledgement and must not treat a normal exit as completion.
      assert [{_seq, {^tag, 1}, _ref, ^holder, {:monitor, :ack}}] = Store.leased(table(tag))

      send(holder, {:finish, :normal})
      assert wait_until(fn -> Tuplex.rdp({tag, :_}) == {:ok, {tag, 1}} end)
    end
  end

  describe "option validation" do
    test "rejects an unknown lease mode", %{tag: tag} do
      assert_raise ArgumentError, ~r/:lease must be false, :monitor/, fn ->
        Tuplex.in({tag, :_}, lease: {:monitor, :later})
      end

      assert_raise ArgumentError, ~r/:lease must be false, :monitor/, fn ->
        Tuplex.in({tag, :_}, lease: true)
      end
    end
  end

  # -- helpers ----------------------------------------------------------------

  defp spawn_holder(tag) do
    parent = self()

    spawn(fn ->
      send(parent, {:took, self(), Tuplex.in({tag, :_}, lease: {:monitor, :ack})})

      receive do
        {:finish, reason} -> exit(reason)
      end
    end)
  end

  defp table(tag) do
    {:ok, _pid, tab} = Shard.lookup(tag)
    tab
  end

  defp waiter_count(pid) do
    pid |> :sys.get_state() |> Map.fetch!(:waiters) |> Map.values() |> List.flatten() |> length()
  end

  defp unique_tag do
    String.to_atom("tuplex_ack_tag_#{System.unique_integer([:positive])}")
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
