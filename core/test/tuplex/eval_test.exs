defmodule Tuplex.EvalTest do
  use ExUnit.Case, async: true

  alias Tuplex.Shard

  setup do
    {:ok, tag: unique_tag()}
  end

  describe "eval/1" do
    test "writes what the function returns", %{tag: tag} do
      {:ok, _pid} = Tuplex.eval(fn -> {tag, :computed} end)

      assert {:ok, {^tag, :computed}} = Tuplex.in({tag, :_}, timeout: 1_000)
    end

    test "returns immediately rather than waiting for the result", %{tag: tag} do
      parent = self()

      {:ok, _pid} =
        Tuplex.eval(fn ->
          receive do
            :go -> {tag, :late}
          end
        end)

      # The call has already returned even though the computation has not started producing.
      assert :empty = Tuplex.rdp({tag, :_})
      send(parent, :noop)
    end

    test "a waiting consumer is woken by the result", %{tag: tag} do
      waiter = Task.async(fn -> Tuplex.in({tag, :_}, timeout: 2_000) end)

      {:ok, _pid} = Tuplex.eval(fn -> {tag, 42} end)

      assert {:ok, {^tag, 42}} = Task.await(waiter)
    end

    test "several evals write several tuples", %{tag: tag} do
      for n <- 1..3, do: Tuplex.eval(fn -> {tag, n} end)

      assert wait_until(fn -> length(Tuplex.rd_all({tag, :_})) == 3 end)
      assert Enum.sort(Tuplex.rd_all({tag, :_})) == [{tag, 1}, {tag, 2}, {tag, 3}]
    end
  end

  # eval has no failure channel on purpose: the arity of what it produces is whatever the
  # function returns, so an error result would have no shape a consumer could match.
  describe "when the function fails" do
    setup do
      parent = self()
      handler = {__MODULE__, System.unique_integer([:positive])}

      :telemetry.attach(
        handler,
        [:tuplex, :eval, :exception],
        fn _event, measurements, metadata, _config ->
          send(parent, {:eval_failed, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)
      :ok
    end

    test "nothing is written and telemetry says so", %{tag: tag} do
      spawn(fn -> Tuplex.eval(fn -> raise "no" end) end)

      assert_receive {:eval_failed, %{count: 1}, %{kind: :error, reason: %RuntimeError{}}}, 1_000
      assert :empty = Tuplex.rdp({tag, :_})
    end

    test "a result that is not a valid tuple fails the same way" do
      spawn(fn -> Tuplex.eval(fn -> "not a tuple" end) end)

      assert_receive {:eval_failed, %{count: 1}, %{kind: :error, reason: %ArgumentError{}}}, 1_000
    end

    test "a thrown value is reported too" do
      spawn(fn -> Tuplex.eval(fn -> throw(:nope) end) end)

      assert_receive {:eval_failed, %{count: 1}, %{kind: :throw, reason: :nope}}, 1_000
    end

    test "the metadata carries a stacktrace" do
      spawn(fn -> Tuplex.eval(fn -> raise "no" end) end)

      assert_receive {:eval_failed, _measurements, %{stacktrace: stacktrace}}, 1_000
      assert is_list(stacktrace)
    end

    test "a failure does not disturb the space", %{tag: tag} do
      Tuplex.out({tag, :untouched})
      spawn(fn -> Tuplex.eval(fn -> raise "no" end) end)
      assert_receive {:eval_failed, _measurements, _metadata}, 1_000

      assert [{^tag, :untouched}] = Tuplex.rd_all({tag, :_})
    end
  end

  describe "rd_all/1" do
    test "returns every match, oldest first", %{tag: tag} do
      for n <- 1..3, do: Tuplex.out({tag, n})

      assert [{^tag, 1}, {^tag, 2}, {^tag, 3}] = Tuplex.rd_all({tag, :_})
    end

    test "leaves them all in place", %{tag: tag} do
      Tuplex.out({tag, 1})

      assert [{^tag, 1}] = Tuplex.rd_all({tag, :_})
      assert [{^tag, 1}] = Tuplex.rd_all({tag, :_})
    end

    test "returns [] for an unseen tag" do
      assert [] = Tuplex.rd_all({unique_tag(), :_})
    end

    test "includes duplicates once each", %{tag: tag} do
      for _ <- 1..3, do: Tuplex.out({tag, :token})

      assert [{^tag, :token}, {^tag, :token}, {^tag, :token}] = Tuplex.rd_all({tag, :_})
    end

    test "does not include leased tuples", %{tag: tag} do
      Tuplex.out({tag, 1})
      Tuplex.out({tag, 2})
      {:ok, _tuple, _handle} = Tuplex.inp({tag, :_}, lease: {:monitor, :ack})

      assert [{^tag, 2}] = Tuplex.rd_all({tag, :_})
    end

    test "rejects an invalid template", %{tag: tag} do
      assert_raise ArgumentError, fn -> Tuplex.rd_all({tag, %{a: :_}}) end
    end

    test "supports the whole-space sweep tags/0 is for", %{tag: tag} do
      Tuplex.out({tag, :a})
      Tuplex.out({tag, :b})

      found = Enum.flat_map(Tuplex.tags(), fn t -> Tuplex.rd_all({t, :_}) end)

      assert {tag, :a} in found
      assert {tag, :b} in found
    end
  end

  defp unique_tag do
    String.to_atom("tuplex_eval_tag_#{System.unique_integer([:positive])}")
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
