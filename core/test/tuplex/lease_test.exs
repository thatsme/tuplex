defmodule Tuplex.LeaseTest do
  use ExUnit.Case, async: true

  alias Tuplex.Shard
  alias Tuplex.Store

  setup do
    tag = unique_tag()
    {:ok, pid} = Shard.ensure(tag)
    {:ok, tag: tag, shard: pid}
  end

  describe "holding a lease" do
    test "the tuple stays in the table, marked", %{tag: tag} do
      Tuplex.out({tag, 1})

      holder =
        spawn_holder(tag, fn ->
          assert [{_seq, {^tag, 1}, _ref, holder_pid, :monitor}] = Store.leased(table(tag))
          assert holder_pid == self()
        end)

      assert_took(holder, {tag, 1})
      assert Store.size(table(tag)) == 1
    end

    test "a leased tuple is invisible to readers and other takers", %{tag: tag} do
      Tuplex.out({tag, 1})
      holder = spawn_holder(tag)
      assert_took(holder, {tag, 1})

      assert :empty = Tuplex.rdp({tag, :_})
      assert :empty = Tuplex.inp({tag, :_})
      assert [] = Shard.read_all({tag, :_})
      assert {:error, :timeout} = Tuplex.rd({tag, :_}, timeout: 20)
    end
  end

  describe "releasing" do
    test "a normal exit discards the tuple", %{tag: tag, shard: shard} do
      Tuplex.out({tag, 1})
      holder = spawn_holder(tag)
      assert_took(holder, {tag, 1})

      finish(holder, :normal)

      assert wait_until(fn -> Store.size(table(tag)) == 0 end)
      assert :empty = Tuplex.rdp({tag, :_})
      assert %{leases: leases} = :sys.get_state(shard)
      assert leases == %{}
    end
  end

  describe "requeueing" do
    for reason <- [:boom, :kill, :shutdown, {:shutdown, :rebalance}] do
      test "an exit with #{inspect(reason)} returns the tuple", %{tag: tag} do
        Tuplex.out({tag, 1})
        holder = spawn_holder(tag)
        assert_took(holder, {tag, 1})

        finish(holder, unquote(Macro.escape(reason)))

        assert wait_until(fn -> Tuplex.rdp({tag, :_}) == {:ok, {tag, 1}} end),
               "tuple was not requeued"
      end
    end

    test "the requeued tuple is free, not leased", %{tag: tag, shard: shard} do
      Tuplex.out({tag, 1})
      holder = spawn_holder(tag)
      assert_took(holder, {tag, 1})
      finish(holder, :boom)

      assert wait_until(fn -> Store.leased(table(tag)) == [] end)
      assert {:ok, {^tag, 1}} = Tuplex.inp({tag, :_})
      assert %{leases: leases} = :sys.get_state(shard)
      assert leases == %{}
    end

    test "it goes to the back of the queue, not the front", %{tag: tag} do
      Tuplex.out({tag, :first})
      Tuplex.out({tag, :second})

      holder = spawn_holder(tag)
      assert_took(holder, {tag, :first})
      finish(holder, :boom)

      assert wait_until(fn -> length(Shard.read_all({tag, :_})) == 2 end)

      # :first was taken and came back, so it now sits behind :second — a tuple that
      # crashes its taker starves rather than looping straight back to the next one.
      assert [{^tag, :second}, {^tag, :first}] = Shard.read_all({tag, :_})
    end

    test "a blocked waiter is offered the requeued tuple", %{tag: tag, shard: shard} do
      Tuplex.out({tag, 1})
      holder = spawn_holder(tag)
      assert_took(holder, {tag, 1})

      waiter = Task.async(fn -> Tuplex.in({tag, :_}) end)
      assert wait_until(fn -> waiter_count(shard) == 1 end)

      finish(holder, :boom)

      assert {:ok, {^tag, 1}} = Task.await(waiter)
    end
  end

  describe "leasing a tuple that arrives later" do
    test "a blocked in/2 with a lease holds what it is given", %{tag: tag, shard: shard} do
      holder = spawn_holder(tag)
      assert wait_until(fn -> waiter_count(shard) == 1 end)

      Tuplex.out({tag, 1})
      assert_took(holder, {tag, 1})

      # Marked in place rather than removed: still one row, and it is held.
      assert Store.size(table(tag)) == 1
      assert [{_seq, {^tag, 1}, _ref, _pid, :monitor}] = Store.leased(table(tag))

      finish(holder, :boom)
      assert wait_until(fn -> Tuplex.rdp({tag, :_}) == {:ok, {tag, 1}} end)
    end
  end

  describe "inp/2 with a lease" do
    test "leases without blocking", %{tag: tag} do
      Tuplex.out({tag, 1})

      parent = self()

      holder =
        spawn(fn ->
          send(parent, {:took, self(), Tuplex.inp({tag, :_}, lease: :monitor)})
          park()
        end)

      assert_receive {:took, ^holder, {:ok, {^tag, 1}}}
      assert :empty = Tuplex.rdp({tag, :_})

      Process.exit(holder, :boom)
      assert wait_until(fn -> Tuplex.rdp({tag, :_}) == {:ok, {tag, 1}} end)
    end
  end

  describe "option validation" do
    test "rd/2 refuses a lease", %{tag: tag} do
      assert_raise ArgumentError, ~r/:lease applies to in\/2 and inp\/2 only/, fn ->
        Tuplex.rd({tag, :_}, lease: :monitor)
      end
    end

    test "rejects a non-boolean lease", %{tag: tag} do
      assert_raise ArgumentError, ~r/:lease must be false, :monitor/, fn ->
        Tuplex.in({tag, :_}, lease: :yes)
      end
    end
  end

  # -- helpers ----------------------------------------------------------------

  # A holder takes a tuple under a lease, reports it, and then parks — which is the window
  # the lease exists for, between the take returning and the process exiting.
  defp spawn_holder(tag, after_take \\ fn -> :ok end) do
    parent = self()

    spawn(fn ->
      result = Tuplex.in({tag, :_}, lease: :monitor)
      after_take.()
      send(parent, {:took, self(), result})
      park()
    end)
  end

  defp park do
    receive do
      {:finish, :normal} -> exit(:normal)
      {:finish, :kill} -> Process.exit(self(), :kill)
      {:finish, reason} -> exit(reason)
    end
  end

  defp finish(holder, reason) do
    ref = Process.monitor(holder)
    send(holder, {:finish, reason})
    assert_receive {:DOWN, ^ref, :process, ^holder, _reason}, 1_000
  end

  defp assert_took(holder, tuple) do
    assert_receive {:took, ^holder, {:ok, ^tuple}}, 1_000
  end

  defp table(tag) do
    {:ok, _pid, tab} = Shard.lookup(tag)
    tab
  end

  defp waiter_count(pid) do
    pid |> :sys.get_state() |> Map.fetch!(:waiters) |> Map.values() |> List.flatten() |> length()
  end

  defp unique_tag do
    String.to_atom("tuplex_lease_tag_#{System.unique_integer([:positive])}")
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
