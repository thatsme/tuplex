defmodule Tuplex.WaiterPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Tuplex.Shard
  alias Tuplex.Template

  # The step-3 property, and the reason key/1 was property-tested in step 1.
  #
  # With N producers and M blocked consumers over overlapping templates:
  #
  #   * every tuple written is delivered to at most one `in` waiter, and none is lost —
  #     what was taken plus what remains equals what was written;
  #   * no consumer is left blocked while a tuple it matches sits in the space, which is
  #     the clause that catches bucket-routing bugs;
  #   * every `rd` waiter whose template matched a written tuple was woken, which is the
  #     clause that catches serving the taker before the readers.
  #
  # There is no settling window anywhere in this test. `out` serves waiters inside the call,
  # so once the last one returns, the shard's waiter index says exactly who is still
  # blocked — the outcome is a fact about state, not a race against a timer.
  property "tuples reach exactly one taker, and no waiter blocks on a tuple that is present" do
    check all(
            writes <- list_of(tuple_spec(), min_length: 1, max_length: 6),
            consumers <- list_of(consumer_spec(), min_length: 1, max_length: 6),
            max_runs: 40
          ) do
      tag = unique_tag()
      {:ok, shard} = Shard.ensure(tag)

      waiting =
        for {mode, pattern} <- consumers do
          template = tagged(pattern, tag)
          {mode, template, block_on(mode, template)}
        end

      assert wait_until(fn -> length(waiter_list(shard)) == length(consumers) end),
             "consumers did not all register"

      tuples = Enum.map(writes, &tagged(&1, tag))
      Enum.each(tuples, &Tuplex.out/1)

      outcomes = settle(waiting, shard)
      remaining = Shard.read_all({tag, :_, :_})
      taken = for {:in, _template, _task, {:ok, tuple}} <- outcomes, do: tuple

      assert Enum.sort(taken ++ remaining) == Enum.sort(tuples),
             "tuples were duplicated or lost"

      for {_mode, template, _task, :blocked} <- outcomes do
        refute Enum.any?(remaining, &Template.matches?(template, &1)),
               "a waiter on #{inspect(template)} is blocked while #{inspect(remaining)} sits in the space"
      end

      for {:rd, template, _task, outcome} <- outcomes,
          Enum.any?(tuples, &Template.matches?(template, &1)) do
        assert {:ok, _tuple} = outcome,
               "a reader on #{inspect(template)} was never woken by #{inspect(tuples)}"
      end

      # Every abandoned waiter must be swept, or the index leaks under exactly this load.
      for {_mode, _template, task, :blocked} <- outcomes do
        Task.shutdown(task, :brutal_kill)
      end

      assert wait_until(fn -> waiter_list(shard) == [] end),
             "abandoned waiters were left in the index"
    end
  end

  defp block_on(:in, template), do: Task.async(fn -> Tuplex.in(template) end)
  defp block_on(:rd, template), do: Task.async(fn -> Tuplex.rd(template) end)

  # Partitions consumers into served and still-blocked using the shard's own index rather
  # than a timeout. Each waiter record carries the pid that registered it, so the mapping
  # back to tasks is exact — two waiters sharing a mode and template are interchangeable
  # for the assertions below, but not for deciding which task to await.
  defp settle(waiting, shard) do
    blocked = shard |> waiter_list() |> MapSet.new(& &1.pid)

    for {mode, template, task} <- waiting do
      if MapSet.member?(blocked, task.pid) do
        {mode, template, task, :blocked}
      else
        {mode, template, task, Task.await(task, 1_000)}
      end
    end
  end

  defp tagged({a, b}, tag), do: {tag, a, b}

  defp colour, do: member_of([:red, :green, :blue])
  defp size, do: member_of([1, 2])

  defp tuple_spec, do: StreamData.tuple({colour(), size()})

  defp consumer_spec do
    StreamData.tuple({member_of([:in, :rd]), pattern_spec()})
  end

  defp pattern_spec do
    StreamData.tuple({
      one_of([colour(), constant(:_)]),
      one_of([size(), constant(:_)])
    })
  end

  defp waiter_list(pid) do
    pid |> :sys.get_state() |> Map.fetch!(:waiters) |> Map.values() |> List.flatten()
  end

  defp unique_tag do
    String.to_atom("tuplex_prop_tag_#{System.unique_integer([:positive])}")
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
