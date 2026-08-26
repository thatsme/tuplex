defmodule Tuplex.LeaseVisibilityTest do
  use ExUnit.Case, async: true

  alias Tuplex.Shard

  setup do
    tag = unique_tag()
    {:ok, pid} = Shard.ensure(tag)
    {:ok, tag: tag, shard: pid}
  end

  # A leased tuple is invisible to readers, which means a blocked rd/2 can sit waiting while
  # a tuple that matches its template is sitting in the space, held. That is correct by
  # design, but it makes the two ways a lease can end observably different, and the
  # difference is what a caller has to reason about.
  describe "a blocked rd/2 against a leased tuple" do
    test "does not see it while it is held", %{tag: tag, shard: shard} do
      Tuplex.out({tag, 1})
      holder = spawn_holder(tag)
      assert_receive {:took, ^holder, {:ok, {^tag, 1}}}, 1_000

      reader = Task.async(fn -> Tuplex.rd({tag, :_}) end)
      assert wait_until(fn -> waiter_count(shard) == 1 end)

      # The tuple exists. The reader is blocked anyway.
      assert Tuplex.rd_all({tag, :_}) == []
      assert nil == Task.yield(reader, 100)

      Task.shutdown(reader, :brutal_kill)
    end

    test "is woken when the lease is requeued", %{tag: tag, shard: shard} do
      Tuplex.out({tag, 1})
      holder = spawn_holder(tag)
      assert_receive {:took, ^holder, {:ok, {^tag, 1}}}, 1_000

      reader = Task.async(fn -> Tuplex.rd({tag, :_}) end)
      assert wait_until(fn -> waiter_count(shard) == 1 end)

      # A requeue is a fresh arrival, so it runs the waiter scan like any other. If it did
      # not, the reader would block forever on a tuple that had come back into the space.
      Process.exit(holder, :kill)

      assert {:ok, {^tag, 1}} = Task.await(reader, 1_000)
    end

    test "never sees it when the lease is acknowledged instead", %{tag: tag, shard: shard} do
      Tuplex.out({tag, 1})
      holder = spawn_acking_holder(tag)
      assert_receive {:took, ^holder, {:ok, {^tag, 1}, _handle}}, 1_000

      reader = Task.async(fn -> Tuplex.rd({tag, :_}) end)
      assert wait_until(fn -> waiter_count(shard) == 1 end)

      send(holder, :ack)

      # The tuple was consumed, so it never becomes visible and the reader keeps waiting.
      # This is the asymmetry: a reader blocked on a leased tuple is woken by a requeue and
      # not by a completion.
      assert nil == Task.yield(reader, 150)
      assert Tuplex.rd_all({tag, :_}) == []

      Task.shutdown(reader, :brutal_kill)
    end

    test "a blocked in/2 behaves the same way", %{tag: tag, shard: shard} do
      Tuplex.out({tag, 1})
      holder = spawn_holder(tag)
      assert_receive {:took, ^holder, {:ok, {^tag, 1}}}, 1_000

      taker = Task.async(fn -> Tuplex.in({tag, :_}) end)
      assert wait_until(fn -> waiter_count(shard) == 1 end)

      Process.exit(holder, :kill)

      assert {:ok, {^tag, 1}} = Task.await(taker, 1_000)
    end
  end

  defp spawn_holder(tag) do
    parent = self()

    spawn(fn ->
      send(parent, {:took, self(), Tuplex.in({tag, :_}, lease: :monitor)})
      Process.sleep(:infinity)
    end)
  end

  defp spawn_acking_holder(tag) do
    parent = self()

    spawn(fn ->
      {:ok, tuple, handle} = Tuplex.in({tag, :_}, lease: {:monitor, :ack})
      send(parent, {:took, self(), {:ok, tuple, handle}})

      receive do
        :ack -> Tuplex.ack(handle)
      end

      Process.sleep(:infinity)
    end)
  end

  defp waiter_count(pid), do: pid |> :sys.get_state() |> Map.fetch!(:waiting)

  defp unique_tag do
    String.to_atom("tuplex_visibility_tag_#{System.unique_integer([:positive])}")
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
