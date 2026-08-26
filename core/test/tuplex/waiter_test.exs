defmodule Tuplex.WaiterTest do
  use ExUnit.Case, async: true

  alias Tuplex.Shard
  alias Tuplex.Store

  setup do
    tag = unique_tag()
    {:ok, pid} = Shard.ensure(tag)
    {:ok, tag: tag, shard: pid}
  end

  describe "in/2" do
    test "returns immediately when a matching tuple is already there", %{tag: tag} do
      Tuplex.out({tag, 1})
      assert {:ok, {^tag, 1}} = Tuplex.in({tag, :_})
      assert waiters(tag) == []
    end

    test "blocks until a matching tuple arrives", %{tag: tag, shard: shard} do
      task = Task.async(fn -> Tuplex.in({tag, :_}) end)
      assert_registered(shard, 1)

      Tuplex.out({tag, 7})
      assert {:ok, {^tag, 7}} = Task.await(task)
    end

    test "consumes the tuple it is given", %{tag: tag, shard: shard} do
      task = Task.async(fn -> Tuplex.in({tag, :_}) end)
      assert_registered(shard, 1)

      Tuplex.out({tag, 7})
      assert {:ok, _} = Task.await(task)
      assert :empty = Tuplex.rdp({tag, :_})
    end

    test "only one of several waiters gets a given tuple", %{tag: tag, shard: shard} do
      tasks = for _ <- 1..3, do: Task.async(fn -> Tuplex.in({tag, :_}) end)
      assert_registered(shard, 3)

      Tuplex.out({tag, 7})

      results = Task.yield_many(tasks, 500)
      served = for {_task, {:ok, {:ok, tuple}}} <- results, do: tuple

      assert served == [{tag, 7}]

      for {task, nil} <- results, do: Task.shutdown(task, :brutal_kill)
    end

    test "a non-matching template in the same bucket is not served", %{tag: tag, shard: shard} do
      # Same key — same tag, same arity — but only one template matches.
      hit = Task.async(fn -> Tuplex.in({tag, :wanted}) end)
      miss = Task.async(fn -> Tuplex.in({tag, :other}) end)
      assert_registered(shard, 2)

      Tuplex.out({tag, :wanted})

      assert {:ok, {^tag, :wanted}} = Task.await(hit)
      assert nil == Task.yield(miss, 100)
      Task.shutdown(miss, :brutal_kill)
    end

    test "waiters are served in arrival order", %{tag: tag, shard: shard} do
      first = Task.async(fn -> Tuplex.in({tag, :_}) end)
      assert_registered(shard, 1)
      second = Task.async(fn -> Tuplex.in({tag, :_}) end)
      assert_registered(shard, 2)

      Tuplex.out({tag, :a})
      assert {:ok, {^tag, :a}} = Task.await(first)
      assert nil == Task.yield(second, 100)

      Tuplex.out({tag, :b})
      assert {:ok, {^tag, :b}} = Task.await(second)
    end
  end

  describe "rd/2" do
    test "blocks until a matching tuple arrives, and leaves it", %{tag: tag, shard: shard} do
      task = Task.async(fn -> Tuplex.rd({tag, :_}) end)
      assert_registered(shard, 1)

      Tuplex.out({tag, 7})

      assert {:ok, {^tag, 7}} = Task.await(task)
      assert {:ok, {^tag, 7}} = Tuplex.rdp({tag, :_})
    end

    test "every matching reader is woken, not just one", %{tag: tag, shard: shard} do
      tasks = for _ <- 1..5, do: Task.async(fn -> Tuplex.rd({tag, :_}) end)
      assert_registered(shard, 5)

      Tuplex.out({tag, 7})

      assert Enum.map(tasks, &Task.await/1) == List.duplicate({:ok, {tag, 7}}, 5)
    end
  end

  # The ordering rule. Satisfying the taker first would delete the tuple while readers that
  # legitimately matched it were still blocked, and they would go on waiting for a tuple
  # that had already been and gone.
  describe "readers are served before the taker" do
    test "a reader and a taker on one tuple are both satisfied", %{tag: tag, shard: shard} do
      reader = Task.async(fn -> Tuplex.rd({tag, :_}) end)
      taker = Task.async(fn -> Tuplex.in({tag, :_}) end)
      assert_registered(shard, 2)

      Tuplex.out({tag, 7})

      assert {:ok, {^tag, 7}} = Task.await(reader)
      assert {:ok, {^tag, 7}} = Task.await(taker)
      assert :empty = Tuplex.rdp({tag, :_})
    end

    test "readers registered after the taker are still served first", %{tag: tag, shard: shard} do
      taker = Task.async(fn -> Tuplex.in({tag, :_}) end)
      assert_registered(shard, 1)

      readers = for _ <- 1..3, do: Task.async(fn -> Tuplex.rd({tag, :_}) end)
      assert_registered(shard, 4)

      Tuplex.out({tag, 7})

      assert {:ok, {^tag, 7}} = Task.await(taker)
      assert Enum.map(readers, &Task.await/1) == List.duplicate({:ok, {tag, 7}}, 3)
    end

    test "a second taker keeps waiting", %{tag: tag, shard: shard} do
      # Registered one at a time: tasks spawned together race, and which of the two takers
      # is "first" is exactly what this test is about.
      reader = Task.async(fn -> Tuplex.rd({tag, :_}) end)
      assert_registered(shard, 1)
      first = Task.async(fn -> Tuplex.in({tag, :_}) end)
      assert_registered(shard, 2)
      second = Task.async(fn -> Tuplex.in({tag, :_}) end)
      assert_registered(shard, 3)

      Tuplex.out({tag, 7})

      assert {:ok, _} = Task.await(reader)
      assert {:ok, _} = Task.await(first)
      assert nil == Task.yield(second, 100)
      Task.shutdown(second, :brutal_kill)
    end
  end

  # A tuple must exist in the table before anyone is served, even when a waiter takes it in
  # the same breath. Otherwise the sequence accounting has a hole in it.
  describe "insert before serve" do
    test "an immediately taken tuple still consumes its sequence number", %{
      tag: tag,
      shard: shard
    } do
      task = Task.async(fn -> Tuplex.in({tag, :_}) end)
      assert_registered(shard, 1)

      assert {:ok, 1} = Shard.out({tag, :first})
      assert {:ok, _} = Task.await(task)

      # The row was inserted under 1 and then deleted, so the next write is 2 — the
      # sequence is never handed out twice.
      assert {:ok, 2} = Shard.out({tag, :second})
      assert [{2, {^tag, :second}}] = Store.to_list(table(tag))
    end
  end

  describe "timeouts" do
    test "report {:error, :timeout} and leave no waiter behind", %{tag: tag, shard: shard} do
      assert {:error, :timeout} = Tuplex.in({tag, :_}, timeout: 20)
      assert_registered(shard, 0)
    end

    test "rd times out the same way", %{tag: tag} do
      assert {:error, :timeout} = Tuplex.rd({tag, :_}, timeout: 20)
    end

    test "a timeout that expires before the tuple arrives does not consume it", %{tag: tag} do
      assert {:error, :timeout} = Tuplex.in({tag, :_}, timeout: 20)
      Tuplex.out({tag, 1})
      assert {:ok, {^tag, 1}} = Tuplex.rdp({tag, :_})
    end

    test "timeout: 0 probes rather than registering a waiter", %{tag: tag, shard: shard} do
      assert {:error, :timeout} = Tuplex.in({tag, :_}, timeout: 0)
      assert {:error, :timeout} = Tuplex.rd({tag, :_}, timeout: 0)
      assert_registered(shard, 0)

      Tuplex.out({tag, 1})
      assert {:ok, {^tag, 1}} = Tuplex.rd({tag, :_}, timeout: 0)
      assert {:ok, {^tag, 1}} = Tuplex.in({tag, :_}, timeout: 0)
      assert :empty = Tuplex.rdp({tag, :_})
    end

    test "rejects a nonsense timeout", %{tag: tag} do
      assert_raise ArgumentError, ~r/:timeout must be/, fn ->
        Tuplex.in({tag, :_}, timeout: -1)
      end

      assert_raise ArgumentError, ~r/:timeout must be/, fn ->
        Tuplex.rd({tag, :_}, timeout: :soon)
      end
    end
  end

  # A waiter left behind by a dead or departed caller would match forever and swallow a
  # tuple meant for a live consumer.
  describe "waiters are not leaked" do
    test "a caller that dies is dropped", %{tag: tag, shard: shard} do
      task = Task.async(fn -> Tuplex.in({tag, :_}) end)
      assert_registered(shard, 1)

      Task.shutdown(task, :brutal_kill)
      assert_registered(shard, 0)

      # The tuple survives for a live consumer instead of vanishing into the dead waiter.
      Tuplex.out({tag, 1})
      assert {:ok, {^tag, 1}} = Tuplex.rdp({tag, :_})
    end

    test "a caller that times out is dropped", %{tag: tag, shard: shard} do
      assert {:error, :timeout} = Tuplex.in({tag, :_}, timeout: 20)
      assert_registered(shard, 0)

      Tuplex.out({tag, 1})
      assert {:ok, {^tag, 1}} = Tuplex.rdp({tag, :_})
    end

    test "serving a waiter removes it and its monitor", %{tag: tag, shard: shard} do
      task = Task.async(fn -> Tuplex.in({tag, :_}) end)
      assert_registered(shard, 1)

      Tuplex.out({tag, 1})
      assert {:ok, _} = Task.await(task)

      state = :sys.get_state(shard)
      assert state.waiters == %{}
      assert state.waiter_index == %{}
      assert state.monitors == %{}
    end
  end

  describe "blocking on an unseen tag" do
    test "starts a shard, because the caller needs somewhere to wait" do
      tag = unique_tag()
      assert :error = Shard.lookup(tag)

      task = Task.async(fn -> Tuplex.in({tag, :_}) end)

      assert wait_until(fn -> match?({:ok, _, _}, Shard.lookup(tag)) end)
      Tuplex.out({tag, 1})
      assert {:ok, {^tag, 1}} = Task.await(task)
    end
  end

  describe "take/2" do
    test "is an alias for in/2", %{tag: tag} do
      Tuplex.out({tag, 1})
      assert {:ok, {^tag, 1}} = Tuplex.take({tag, :_})
      assert {:error, :timeout} = Tuplex.take({tag, :_}, timeout: 20)
    end
  end

  # -- helpers ----------------------------------------------------------------

  defp waiters(tag) do
    {:ok, pid, _tab} = Shard.lookup(tag)
    waiter_list(pid)
  end

  defp waiter_list(pid) do
    pid |> :sys.get_state() |> Map.fetch!(:waiters) |> Map.values() |> List.flatten()
  end

  defp table(tag) do
    {:ok, _pid, tab} = Shard.lookup(tag)
    tab
  end

  # Registration is a call the caller makes from its own process, so a spawned waiter is
  # not filed the instant Task.async returns.
  defp assert_registered(shard, count) do
    assert wait_until(fn -> length(waiter_list(shard)) == count end),
           "expected #{count} waiters, found #{length(waiter_list(shard))}"
  end

  defp unique_tag do
    String.to_atom("tuplex_wait_tag_#{System.unique_integer([:positive])}")
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
