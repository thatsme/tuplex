defmodule Tuplex.WatchTest do
  use ExUnit.Case, async: true

  alias Tuplex.Shard

  setup do
    tag = unique_tag()
    {:ok, pid} = Shard.ensure(tag)
    {:ok, tag: tag, shard: pid}
  end

  describe "watch/2" do
    test "reports tuples as they are written", %{tag: tag} do
      {:ok, ref} = Tuplex.watch({tag, :_})

      Tuplex.out({tag, 1})
      Tuplex.out({tag, 2})

      assert_receive {Shard, :watch, ^ref, :out, {^tag, 1}}
      assert_receive {Shard, :watch, ^ref, :out, {^tag, 2}}
    end

    test "only reports matching tuples", %{tag: tag} do
      {:ok, ref} = Tuplex.watch({tag, :wanted})

      Tuplex.out({tag, :other})
      Tuplex.out({tag, :wanted})

      assert_receive {Shard, :watch, ^ref, :out, {^tag, :wanted}}
      refute_received {Shard, :watch, ^ref, :out, {^tag, :other}}
    end

    test "does not consume", %{tag: tag} do
      {:ok, _ref} = Tuplex.watch({tag, :_})
      Tuplex.out({tag, 1})

      assert {:ok, {^tag, 1}} = Tuplex.rdp({tag, :_})
      assert [{^tag, 1}] = Tuplex.rd_all({tag, :_})
    end

    test "several watchers all hear about the same tuple", %{tag: tag} do
      {:ok, first} = Tuplex.watch({tag, :_})
      {:ok, second} = Tuplex.watch({tag, :_})

      Tuplex.out({tag, 1})

      assert_receive {Shard, :watch, ^first, :out, {^tag, 1}}
      assert_receive {Shard, :watch, ^second, :out, {^tag, 1}}
    end

    test "starts a shard for an unseen tag" do
      tag = unique_tag()
      assert :error = Shard.lookup(tag)

      {:ok, ref} = Tuplex.watch({tag, :_})
      assert {:ok, _pid, _tab} = Shard.lookup(tag)

      Tuplex.out({tag, 1})
      assert_receive {Shard, :watch, ^ref, :out, {^tag, 1}}
    end

    test "rejects an invalid template", %{tag: tag} do
      assert_raise ArgumentError, fn -> Tuplex.watch({tag, :"$1"}) end
    end
  end

  # A watcher is observational, like rd. Because a tuple is written to the table before
  # anything is served, a watcher hears about it even when a blocked in/2 takes it in the
  # same instant — what it is told about genuinely existed in the space.
  describe "watching against a waiting consumer" do
    test "an instantly consumed tuple is still reported", %{tag: tag, shard: shard} do
      {:ok, ref} = Tuplex.watch({tag, :_})

      taker = Task.async(fn -> Tuplex.in({tag, :_}) end)
      assert wait_until(fn -> waiter_count(shard) == 1 end)

      Tuplex.out({tag, 1})

      assert {:ok, {^tag, 1}} = Task.await(taker)
      assert_receive {Shard, :watch, ^ref, :out, {^tag, 1}}
      assert :empty = Tuplex.rdp({tag, :_})
    end
  end

  describe "events" do
    test "only :out by default", %{tag: tag} do
      {:ok, ref} = Tuplex.watch({tag, :_})

      Tuplex.out({tag, 1})
      assert {:ok, _} = Tuplex.inp({tag, :_})

      assert_receive {Shard, :watch, ^ref, :out, {^tag, 1}}
      refute_received {Shard, :watch, ^ref, :in, {^tag, 1}}
    end

    test ":in reports consumption", %{tag: tag} do
      {:ok, ref} = Tuplex.watch({tag, :_}, events: [:out, :in])

      Tuplex.out({tag, 1})
      assert {:ok, _} = Tuplex.inp({tag, :_})

      assert_receive {Shard, :watch, ^ref, :out, {^tag, 1}}
      assert_receive {Shard, :watch, ^ref, :in, {^tag, 1}}
    end

    test ":in reports a tuple taken by a blocked waiter", %{tag: tag, shard: shard} do
      {:ok, ref} = Tuplex.watch({tag, :_}, events: [:out, :in])

      taker = Task.async(fn -> Tuplex.in({tag, :_}) end)
      assert wait_until(fn -> waiter_count(shard) == 1 end)
      Tuplex.out({tag, 1})
      assert {:ok, _} = Task.await(taker)

      # The order matters: it existed before it was taken.
      assert_receive {Shard, :watch, ^ref, :out, {^tag, 1}}
      assert_receive {Shard, :watch, ^ref, :in, {^tag, 1}}
    end

    test ":requeue reports a lease coming back", %{tag: tag} do
      {:ok, ref} = Tuplex.watch({tag, :_}, events: [:requeue])

      Tuplex.out({tag, 1})
      holder = spawn_holder(tag)
      assert_receive {:took, ^holder, {:ok, {^tag, 1}}}, 1_000

      Process.exit(holder, :kill)

      assert_receive {Shard, :watch, ^ref, :requeue, {^tag, 1}}, 1_000
    end

    test "rejects unknown events", %{tag: tag} do
      assert_raise ArgumentError, ~r/unknown watch events/, fn ->
        Tuplex.watch({tag, :_}, events: [:out, :sideways])
      end

      assert_raise ArgumentError, ~r/:events must be a non-empty list/, fn ->
        Tuplex.watch({tag, :_}, events: [])
      end
    end
  end

  # Lossy on purpose. A watcher is pushed at and never asked, so an unbounded send to a slow
  # subscriber is an unbounded mailbox the shard cannot see.
  describe "backpressure" do
    test "events past the queue threshold are dropped and reported", %{tag: tag} do
      parent = self()

      # A subscriber that never reads its mailbox, with a threshold of zero, so the first
      # message it is already holding puts it over.
      subscriber = spawn(fn -> Process.sleep(:infinity) end)
      on_exit(fn -> Process.exit(subscriber, :kill) end)
      send(subscriber, :backlog)

      {:ok, ref} = Tuplex.watch({tag, :_}, subscriber: subscriber, max_queue: 0)

      handler = {__MODULE__, ref}

      :telemetry.attach(
        handler,
        [:tuplex, :watch, :dropped],
        fn _event, measurements, metadata, _config ->
          send(parent, {:dropped, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      Tuplex.out({tag, 1})

      assert_receive {:dropped, %{count: 1, message_queue_len: len}, metadata}
      assert len > 0
      assert metadata.tag == tag
      assert metadata.ref == ref
      assert metadata.event == :out
    end

    test "a subscriber under the threshold still receives", %{tag: tag} do
      {:ok, ref} = Tuplex.watch({tag, :_}, max_queue: 10_000)
      Tuplex.out({tag, 1})
      assert_receive {Shard, :watch, ^ref, :out, {^tag, 1}}
    end
  end

  describe "unwatch/1" do
    test "stops the subscription", %{tag: tag, shard: shard} do
      {:ok, ref} = Tuplex.watch({tag, :_})
      Tuplex.out({tag, 1})
      assert_receive {Shard, :watch, ^ref, :out, {^tag, 1}}

      assert :ok = Tuplex.unwatch(ref)
      assert wait_until(fn -> watch_count(shard) == 0 end)

      Tuplex.out({tag, 2})
      refute_receive {Shard, :watch, ^ref, :out, {^tag, 2}}, 100
    end

    test "is fine on an unknown ref" do
      assert :ok = Tuplex.unwatch(make_ref())
    end
  end

  describe "a subscriber that dies" do
    test "has its registration dropped", %{tag: tag, shard: shard} do
      subscriber = spawn(fn -> Process.sleep(:infinity) end)
      {:ok, _ref} = Tuplex.watch({tag, :_}, subscriber: subscriber)
      assert wait_until(fn -> watch_count(shard) == 1 end)

      Process.exit(subscriber, :kill)

      assert wait_until(fn -> watch_count(shard) == 0 end),
             "the watch registration outlived its audience"
    end
  end

  # A watcher is not sitting in a call, so it cannot notice a dead shard and re-register the
  # way a blocked caller does. Tuplex.Watch is what does it for them.
  describe "surviving a shard crash" do
    test "the subscription is re-registered with the replacement", %{tag: tag, shard: shard} do
      {:ok, ref} = Tuplex.watch({tag, :_})

      down = Process.monitor(shard)
      Process.exit(shard, :kill)
      assert_receive {:DOWN, ^down, :process, ^shard, :killed}

      assert wait_until(fn ->
               match?({:ok, pid, _tab} when pid != shard, Shard.lookup(tag))
             end)

      assert wait_until(fn ->
               {:ok, pid, _tab} = Shard.lookup(tag)
               watch_count(pid) == 1
             end),
             "the watch was not re-registered"

      Tuplex.out({tag, 1})
      assert_receive {Shard, :watch, ^ref, :out, {^tag, 1}}, 1_000
    end
  end

  # -- helpers ----------------------------------------------------------------

  defp spawn_holder(tag) do
    parent = self()

    spawn(fn ->
      send(parent, {:took, self(), Tuplex.in({tag, :_}, lease: :monitor)})
      Process.sleep(:infinity)
    end)
  end

  defp waiter_count(pid), do: bucket_count(pid, :waiters)
  defp watch_count(pid), do: bucket_count(pid, :watches)

  defp bucket_count(pid, field) do
    pid |> :sys.get_state() |> Map.fetch!(field) |> Map.values() |> List.flatten() |> length()
  end

  defp unique_tag do
    String.to_atom("tuplex_watch_tag_#{System.unique_integer([:positive])}")
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
