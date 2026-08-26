defmodule Tuplex.TelemetryTest do
  use ExUnit.Case, async: true

  alias Tuplex.Shard
  alias Tuplex.Telemetry

  setup do
    tag = unique_tag()
    {:ok, pid} = Shard.ensure(tag)
    {:ok, tag: tag, shard: pid}
  end

  describe "events/0" do
    test "every documented event is listed" do
      assert [:tuplex, :out] in Telemetry.events()
      assert [:tuplex, :in, :stop] in Telemetry.events()
      assert [:tuplex, :shard, :stats] in Telemetry.events()
    end

    test "the list has no duplicates" do
      assert Enum.uniq(Telemetry.events()) == Telemetry.events()
    end

    test "every name is a list of atoms starting with :tuplex" do
      for event <- Telemetry.events() do
        assert [:tuplex | rest] = event
        assert Enum.all?(rest, &is_atom/1)
      end
    end
  end

  describe "[:tuplex, :out]" do
    test "fires once per write, with the space it landed in", %{tag: tag} do
      attach([[:tuplex, :out]], tag)

      Tuplex.out({tag, 1})

      assert_receive {:event, [:tuplex, :out], measurements, metadata}
      assert measurements.count == 1
      assert measurements.space_size == 1
      assert measurements.waiter_count == 0
      assert metadata.tag == tag
      assert metadata.arity == 2
    end

    test "reports the waiters it served", %{tag: tag, shard: shard} do
      task = Task.async(fn -> Tuplex.in({tag, :_}) end)
      assert wait_until(fn -> waiter_count(shard) == 1 end)

      attach([[:tuplex, :out]], tag)
      Tuplex.out({tag, 1})

      # The waiter was served during the call, so by the time the event fires it is gone and
      # the tuple with it.
      assert_receive {:event, [:tuplex, :out], measurements, _metadata}
      assert measurements.waiter_count == 0
      assert measurements.space_size == 0
      assert {:ok, _} = Task.await(task)
    end
  end

  describe "[:tuplex, :inp]" do
    test "carries the result and the lease mode", %{tag: tag} do
      Tuplex.out({tag, 1})
      attach([[:tuplex, :inp]], tag)

      assert {:ok, _} = Tuplex.inp({tag, :_})

      assert_receive {:event, [:tuplex, :inp], measurements, metadata}
      assert measurements.space_size == 0
      assert metadata.result == :ok
      assert metadata.lease == false
      assert metadata.arity == 2
    end

    test "fires on an empty space too", %{tag: tag} do
      attach([[:tuplex, :inp]], tag)

      assert :empty = Tuplex.inp({tag, :_})

      assert_receive {:event, [:tuplex, :inp], _measurements, %{result: :empty}}
    end

    test "in/2 with a zero timeout does not double-count as an inp", %{tag: tag} do
      Tuplex.out({tag, 1})
      attach([[:tuplex, :inp], [:tuplex, :in, :stop]], tag)

      assert {:ok, _} = Tuplex.in({tag, :_}, timeout: 0)

      # It is already inside its own span; counting it twice would inflate every dashboard
      # that adds up operations.
      assert_receive {:event, [:tuplex, :in, :stop], _measurements, _metadata}
      refute_received {:event, [:tuplex, :inp], _m, _md}
    end
  end

  # Duration is the signal here and nowhere else: it is how long a consumer waited.
  describe "the in/2 and rd/2 spans" do
    test "measure the wait", %{tag: tag, shard: shard} do
      attach([[:tuplex, :in, :start], [:tuplex, :in, :stop]], tag)

      task = Task.async(fn -> Tuplex.in({tag, :_}) end)
      assert wait_until(fn -> waiter_count(shard) == 1 end)
      assert_receive {:event, [:tuplex, :in, :start], _measurements, metadata}
      assert metadata.tag == tag
      assert metadata.timeout == :infinity

      Process.sleep(30)
      Tuplex.out({tag, 1})
      assert {:ok, _} = Task.await(task)

      assert_receive {:event, [:tuplex, :in, :stop], measurements, metadata}
      assert measurements.duration >= System.convert_time_unit(30, :millisecond, :native)
      assert metadata.result == :ok
    end

    test "report a timeout as a result rather than an exception", %{tag: tag} do
      attach([[:tuplex, :in, :stop], [:tuplex, :in, :exception]], tag)

      assert {:error, :timeout} = Tuplex.in({tag, :_}, timeout: 20)

      assert_receive {:event, [:tuplex, :in, :stop], _measurements, %{result: :timeout}}
      refute_received {:event, [:tuplex, :in, :exception], _m, _md}
    end

    test "rd/2 spans the same way", %{tag: tag} do
      attach([[:tuplex, :rd, :stop]], tag)

      Tuplex.out({tag, 1})
      assert {:ok, _} = Tuplex.rd({tag, :_})

      assert_receive {:event, [:tuplex, :rd, :stop], measurements, metadata}
      assert is_integer(measurements.duration)
      assert metadata.result == :ok
    end

    test "an invalid template raises before the span opens", %{tag: tag} do
      attach([[:tuplex, :in, :start], [:tuplex, :in, :exception]], tag)

      assert_raise ArgumentError, fn -> Tuplex.in({tag, :"$1"}) end

      refute_received {:event, [:tuplex, :in, :start], _m, _md}
    end
  end

  # These run in the caller, where the waiter index is not visible. Not faking it is the
  # point: asking the shard would undo the reason the read bypasses it.
  describe "caller-side reads" do
    test "rdp carries space_size but no waiter_count", %{tag: tag} do
      Tuplex.out({tag, 1})
      attach([[:tuplex, :rdp]], tag)

      assert {:ok, _} = Tuplex.rdp({tag, :_})

      assert_receive {:event, [:tuplex, :rdp], measurements, metadata}
      assert measurements.space_size == 1
      refute Map.has_key?(measurements, :waiter_count)
      assert metadata.result == :ok
    end

    test "rd_all reports how many it matched", %{tag: tag} do
      for n <- 1..3, do: Tuplex.out({tag, n})
      Tuplex.out({tag, :a, :b})
      attach([[:tuplex, :rd_all]], tag)

      assert length(Tuplex.rd_all({tag, :_})) == 3

      assert_receive {:event, [:tuplex, :rd_all], measurements, _metadata}
      assert measurements.matched == 3
      assert measurements.space_size == 4
      refute Map.has_key?(measurements, :waiter_count)
    end

    test "an empty read still fires", %{tag: tag} do
      attach([[:tuplex, :rdp]], tag)
      assert :empty = Tuplex.rdp({tag, :_})
      assert_receive {:event, [:tuplex, :rdp], %{space_size: 0}, %{result: :empty}}
    end
  end

  describe "lease events" do
    test "an acknowledgement releases", %{tag: tag} do
      Tuplex.out({tag, 1})
      attach([[:tuplex, :lease, :released], [:tuplex, :lease, :requeued]], tag)

      {:ok, _tuple, handle} = Tuplex.inp({tag, :_}, lease: {:monitor, :ack})
      assert :ok = Tuplex.ack(handle)

      assert_receive {:event, [:tuplex, :lease, :released], measurements, metadata}
      assert measurements.count == 1
      assert metadata.mode == {:monitor, :ack}
      assert metadata.reason == :ack
      assert metadata.arity == 2
      refute_received {:event, [:tuplex, :lease, :requeued], _m, _md}
    end

    test "a dead holder requeues, and says why", %{tag: tag} do
      Tuplex.out({tag, 1})
      attach([[:tuplex, :lease, :requeued]], tag)

      holder = spawn_holder(tag)
      assert_receive {:took, ^holder, _result}, 1_000
      Process.exit(holder, :kill)

      assert_receive {:event, [:tuplex, :lease, :requeued], measurements, metadata}, 1_000
      assert measurements.space_size == 1
      assert metadata.mode == :monitor
      assert metadata.reason == :killed
    end
  end

  describe "[:tuplex, :eval]" do
    test "fires when the result reaches the space", %{tag: tag} do
      attach([[:tuplex, :eval]], tag)

      Tuplex.eval(fn -> {tag, :computed} end)

      assert_receive {:event, [:tuplex, :eval], %{count: 1}, metadata}, 1_000
      assert metadata.tag == tag
      assert metadata.arity == 2
    end

    test "a failure fires the exception event instead", %{tag: tag} do
      attach([[:tuplex, :eval], [:tuplex, :eval, :exception]], tag)

      # An eval that fails before producing a tuple has no tag to filter on, so the message
      # identifies this test to itself.
      message = "boom #{tag}"
      spawn(fn -> Tuplex.eval(fn -> raise message end) end)

      assert_receive {:event, [:tuplex, :eval, :exception], %{count: 1},
                      %{kind: :error, reason: %RuntimeError{message: ^message}}},
                     1_000

      refute_received {:event, [:tuplex, :eval], _m, _md}
      assert :empty = Tuplex.rdp({tag, :_})
    end
  end

  # The event that corresponds to no operation, and the only one that can tell you a shard
  # is starving while nothing at all is happening to it.
  describe "[:tuplex, :shard, :stats]" do
    test "reports the shape of the shard", %{tag: tag, shard: shard} do
      Tuplex.out({tag, 1})
      {:ok, watch} = Tuplex.watch({tag, :_})
      on_exit(fn -> Tuplex.unwatch(watch) end)

      attach([[:tuplex, :shard, :stats]], tag)
      send(shard, :stats)

      assert_receive {:event, [:tuplex, :shard, :stats], measurements, metadata}
      assert metadata.tag == tag
      assert measurements.space_size == 1
      assert measurements.waiter_count == 0
      assert measurements.watch_count == 1
      assert measurements.lease_count == 0
      assert measurements.oldest_waiter_age_ms == 0
    end

    test "counts leases", %{tag: tag, shard: shard} do
      Tuplex.out({tag, 1})
      {:ok, _tuple, _handle} = Tuplex.inp({tag, :_}, lease: {:monitor, :ack})

      attach([[:tuplex, :shard, :stats]], tag)
      send(shard, :stats)

      assert_receive {:event, [:tuplex, :shard, :stats], %{lease_count: 1}, _metadata}
    end

    test "ages the longest-blocked waiter", %{tag: tag, shard: shard} do
      task = Task.async(fn -> Tuplex.in({tag, :_}) end)
      assert wait_until(fn -> waiter_count(shard) == 1 end)
      Process.sleep(30)

      attach([[:tuplex, :shard, :stats]], tag)
      send(shard, :stats)

      assert_receive {:event, [:tuplex, :shard, :stats], measurements, _metadata}
      assert measurements.waiter_count == 1
      assert measurements.oldest_waiter_age_ms >= 25

      Task.shutdown(task, :brutal_kill)
    end

    test "an idle shard reports zeros rather than nothing", %{tag: tag, shard: shard} do
      attach([[:tuplex, :shard, :stats]], tag)
      send(shard, :stats)

      assert_receive {:event, [:tuplex, :shard, :stats], measurements, %{tag: ^tag}}
      assert measurements.space_size == 0
      assert measurements.waiter_count == 0
      assert measurements.oldest_waiter_age_ms == 0
    end
  end

  # waiter_count and watch_count are maintained rather than counted, so that a per-event
  # measurement stays O(1). This is the test that catches them drifting.
  describe "the maintained counters" do
    test "track the buckets through registration, service and death", %{tag: tag, shard: shard} do
      tasks = for _ <- 1..3, do: Task.async(fn -> Tuplex.in({tag, :_}) end)
      assert wait_until(fn -> waiter_count(shard) == 3 end)
      assert_counters(shard)

      Tuplex.out({tag, 1})
      assert wait_until(fn -> waiter_count(shard) == 2 end)
      assert_counters(shard)

      Enum.each(tasks, &Task.shutdown(&1, :brutal_kill))
      assert wait_until(fn -> waiter_count(shard) == 0 end)
      assert_counters(shard)

      refs = for _ <- 1..2, do: elem(Tuplex.watch({tag, :_}), 1)
      assert wait_until(fn -> watch_count(shard) == 2 end)
      assert_counters(shard)

      Enum.each(refs, &Tuplex.unwatch/1)
      assert wait_until(fn -> watch_count(shard) == 0 end)
      assert_counters(shard)
    end
  end

  # -- helpers ----------------------------------------------------------------

  defp assert_counters(shard) do
    state = :sys.get_state(shard)
    assert state.waiting == flatten(state.waiters), "waiting counter drifted"
    assert state.watching == flatten(state.watches), "watching counter drifted"
  end

  defp flatten(buckets), do: buckets |> Map.values() |> List.flatten() |> length()

  # :telemetry handlers are global and this suite is async, so a handler that forwarded
  # everything would pick up events from whatever else is running. Only this tag gets
  # through. Events with no tag at all — an eval that failed before producing a tuple — have
  # nothing to filter on, so those tests identify themselves another way.
  defp attach(events, tag) do
    parent = self()
    handler = {__MODULE__, System.unique_integer([:positive])}

    :telemetry.attach_many(
      handler,
      events,
      fn event, measurements, metadata, _config ->
        if Map.get(metadata, :tag, tag) == tag do
          send(parent, {:event, event, measurements, metadata})
        end
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)
  end

  defp spawn_holder(tag) do
    parent = self()

    spawn(fn ->
      send(parent, {:took, self(), Tuplex.in({tag, :_}, lease: :monitor)})
      Process.sleep(:infinity)
    end)
  end

  defp waiter_count(pid), do: pid |> :sys.get_state() |> Map.fetch!(:waiting)
  defp watch_count(pid), do: pid |> :sys.get_state() |> Map.fetch!(:watching)

  defp unique_tag do
    String.to_atom("tuplex_telemetry_tag_#{System.unique_integer([:positive])}")
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
