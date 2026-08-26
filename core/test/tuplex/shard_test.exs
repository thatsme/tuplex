defmodule Tuplex.ShardTest do
  use ExUnit.Case, async: true

  alias Tuplex.Shard
  alias Tuplex.Store

  setup do
    {:ok, tag: unique_tag()}
  end

  describe "starting on demand" do
    test "out starts a shard for an unseen tag", %{tag: tag} do
      assert :error = Shard.lookup(tag)
      assert {:ok, 1} = Shard.out({tag, 1})
      assert {:ok, pid, tab} = Shard.lookup(tag)
      assert is_pid(pid)
      assert :ets.info(tab, :size) == 1
    end

    test "ensure/1 is idempotent", %{tag: tag} do
      assert {:ok, pid} = Shard.ensure(tag)
      assert {:ok, ^pid} = Shard.ensure(tag)
    end

    test "concurrent starts converge on one shard", %{tag: tag} do
      pids =
        1..20
        |> Task.async_stream(fn _ -> Shard.ensure(tag) end, ordered: false)
        |> Enum.map(fn {:ok, {:ok, pid}} -> pid end)
        |> Enum.uniq()

      assert [_only_one] = pids
    end

    test "reads never start a shard", %{tag: tag} do
      assert :empty = Shard.read({tag, :_})
      assert [] = Shard.read_all({tag, :_})
      assert :empty = Shard.take({tag, :_})
      assert :error = Shard.lookup(tag)
    end

    test "the registry value carries the shard's table", %{tag: tag} do
      {:ok, pid} = Shard.ensure(tag)
      assert [{^pid, tab}] = Registry.lookup(Tuplex.Registry, tag)
      assert :ets.info(tab, :type) == :ordered_set
      assert :ets.info(tab, :protection) == :protected
      assert :ets.info(tab, :owner) == pid
    end
  end

  describe "out/1" do
    test "returns the sequence it wrote under", %{tag: tag} do
      assert {:ok, 1} = Shard.out({tag, :a})
      assert {:ok, 2} = Shard.out({tag, :b})
      assert {:ok, 3} = Shard.out({tag, :c})
    end

    test "sequences keep tuples distinct rather than collapsing them", %{tag: tag} do
      assert {:ok, 1} = Shard.out({tag})
      assert {:ok, 2} = Shard.out({tag})

      assert [{tag}, {tag}] == Shard.read_all({tag})
    end
  end

  describe "routing by tag" do
    test "a tag only sees its own tuples" do
      a = unique_tag()
      b = unique_tag()

      Shard.out({a, 1})
      Shard.out({b, 2})

      assert {:ok, {^a, 1}} = Shard.read({a, :_})
      assert {:ok, {^b, 2}} = Shard.read({b, :_})
      assert [{^a, 1}] = Shard.read_all({a, :_})
    end

    test "each tag gets its own table" do
      a = unique_tag()
      b = unique_tag()

      {:ok, _, tab_a} = Shard.out({a, 1}) && Shard.lookup(a)
      {:ok, _, tab_b} = Shard.out({b, 1}) && Shard.lookup(b)

      refute tab_a == tab_b
    end
  end

  describe "take/1 versus read/1" do
    setup %{tag: tag} do
      Shard.out({tag, 1})
      Shard.out({tag, 2})
      :ok
    end

    test "take removes, read does not", %{tag: tag} do
      assert {:ok, {^tag, 1}} = Shard.read({tag, :_})
      assert {:ok, {^tag, 1}} = Shard.read({tag, :_})

      assert {:ok, {^tag, 1}} = Shard.take({tag, :_})
      assert {:ok, {^tag, 2}} = Shard.read({tag, :_})
    end

    test "take drains in order and then reports empty", %{tag: tag} do
      assert {:ok, {^tag, 1}} = Shard.take({tag, :_})
      assert {:ok, {^tag, 2}} = Shard.take({tag, :_})
      assert :empty = Shard.take({tag, :_})
    end

    test "concurrent takes never hand the same tuple to two callers" do
      tag2 = unique_tag()
      for n <- 1..50, do: Shard.out({tag2, n})

      taken =
        1..80
        |> Task.async_stream(fn _ -> Shard.take({tag2, :_}) end, ordered: false)
        |> Enum.flat_map(fn
          {:ok, {:ok, tuple}} -> [tuple]
          {:ok, :empty} -> []
        end)

      assert length(taken) == 50
      assert length(Enum.uniq(taken)) == 50
    end
  end

  # The whole point of putting the table reference in the registry value. If reads went
  # through the shard, a suspended shard would block them too.
  describe "reads run in the calling process" do
    test "a read succeeds while its shard is suspended", %{tag: tag} do
      Shard.out({tag, 1})
      {:ok, pid, _tab} = Shard.lookup(tag)

      :sys.suspend(pid)

      try do
        assert {:ok, {^tag, 1}} = Shard.read({tag, :_})
        assert [{^tag, 1}] = Shard.read_all({tag, :_})

        # ...while a destructive read, which must be serialised, does block.
        task = Task.async(fn -> Shard.take({tag, :_}) end)
        assert nil == Task.yield(task, 100)
        Task.shutdown(task, :brutal_kill)
      after
        :sys.resume(pid)
      end
    end

    test "a read does not queue behind pending writes", %{tag: tag} do
      Shard.out({tag, 0})
      {:ok, pid, _tab} = Shard.lookup(tag)

      :sys.suspend(pid)

      writers =
        try do
          writers = for n <- 1..10, do: Task.async(fn -> Shard.out({tag, n}) end)
          assert {:ok, {^tag, 0}} = Shard.read({tag, :_})
          writers
        after
          :sys.resume(pid)
        end

      Enum.each(writers, &Task.await/1)
      assert length(Shard.read_all({tag, :_})) == 11
    end
  end

  describe "stale table references" do
    test "a read retries once against the shard's current table", %{tag: tag} do
      Shard.out({tag, 1})
      dead = dead_table()

      assert {:ok, {^tag, 1}} =
               Shard.attempt(tag, dead, :empty, &Store.read(&1, {tag, :_}))

      assert [{^tag, 1}] = Shard.attempt(tag, dead, [], &Store.read_all(&1, {tag, :_}))
    end

    test "falls back to the default when the tag has no shard left", %{tag: tag} do
      dead = dead_table()

      assert :empty = Shard.attempt(tag, dead, :empty, &Store.read(&1, {tag, :_}))
      assert [] = Shard.attempt(tag, dead, [], &Store.read_all(&1, {tag, :_}))
    end

    test "a live shard registered against a table it does not have is left to raise", %{tag: tag} do
      dead = dead_table()

      # Registering from this process makes the test itself the tag's "shard", so the
      # re-lookup hands back the same dead reference and the retry has nowhere to go.
      {:ok, _} = Registry.register(Tuplex.Registry, tag, dead)

      assert_raise ArgumentError, fn ->
        Shard.attempt(tag, dead, :empty, &Store.read(&1, {tag, :_}))
      end
    end

    test "a read spanning a shard's death returns empty rather than raising", %{tag: tag} do
      Shard.out({tag, 1})
      {:ok, pid, _tab} = Shard.lookup(tag)

      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}

      # The shard's table died with it. Whether the registry has been cleaned up yet or a
      # replacement shard is already registered, the answer is an empty space — never a
      # raised ArgumentError from a table that is gone.
      assert :empty = Shard.read({tag, :_})
      assert [] = Shard.read_all({tag, :_})
    end
  end

  describe "tags/0" do
    test "lists a tag once its shard exists", %{tag: tag} do
      refute tag in Shard.tags()
      Shard.out({tag, 1})
      assert tag in Shard.tags()
    end

    test "drops a tag when its shard goes away", %{tag: tag} do
      {:ok, pid} = Shard.ensure(tag)
      assert tag in Shard.tags()

      :ok = DynamicSupervisor.terminate_child(Tuplex.ShardSupervisor, pid)

      # The registry unregisters on the shard's DOWN, which lands after terminate_child has
      # returned, so removal is eventually consistent rather than immediate.
      assert wait_until(fn -> tag not in Shard.tags() end)
    end
  end

  describe "sequence counter" do
    test "starts from the table rather than from zero", %{tag: tag} do
      {:ok, pid} = Shard.ensure(tag)
      assert %{seq: 1} = :sys.get_state(pid)
    end

    test "advances past every write", %{tag: tag} do
      for _ <- 1..5, do: Shard.out({tag, :x})
      {:ok, pid, _tab} = Shard.lookup(tag)
      assert %{seq: 6} = :sys.get_state(pid)
    end
  end

  defp unique_tag do
    String.to_atom("tuplex_test_tag_#{System.unique_integer([:positive])}")
  end

  defp dead_table do
    tab = :ets.new(:dead, [:ordered_set])
    :ets.delete(tab)
    tab
  end

  defp wait_until(fun, timeout \\ 1_000) do
    wait_until(fun, System.monotonic_time(:millisecond) + timeout, timeout)
  end

  defp wait_until(fun, deadline, timeout) do
    cond do
      fun.() ->
        true

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("not settled in #{timeout}ms")

      true ->
        Process.sleep(5)
        wait_until(fun, deadline, timeout)
    end
  end
end
