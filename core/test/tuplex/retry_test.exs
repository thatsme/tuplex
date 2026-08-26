defmodule Tuplex.RetryTest do
  use ExUnit.Case, async: true

  alias Tuplex.Shard

  # These pin the *decision*, not the race.
  #
  # A shard can die between being resolved and being called, and retrying is safe only when
  # the message provably never arrived. `:noproc` is the only exit reason that proves that.
  # Retrying anything else risks a double write: the shard may have handled the call and
  # died afterwards.
  #
  # The double-write window itself is not tested — doing so needs fault injection between a
  # shard receiving a message and replying, which is brittle enough to be deleted within a
  # year. Extracting the predicate means a mutation that reverses the decision is caught at
  # near-zero cost, which is the part that was previously undefended. See
  # `test/MUTATION_LOG.md`.
  describe "retryable_exit?/1" do
    test ":noproc is retryable — the call never reached a process" do
      assert Shard.retryable_exit?({:noproc, {GenServer, :call, [self(), :ping, 5000]}})
    end

    test "every other reason is not" do
      call = {GenServer, :call, [self(), :ping, 5000]}

      refute Shard.retryable_exit?({:normal, call})
      refute Shard.retryable_exit?({:shutdown, call})
      refute Shard.retryable_exit?({{:shutdown, :rebalance}, call})
      refute Shard.retryable_exit?({:killed, call})
      refute Shard.retryable_exit?({:timeout, call})
      refute Shard.retryable_exit?({%RuntimeError{message: "boom"}, call})
    end

    test "a bare reason with no call context is not retryable" do
      refute Shard.retryable_exit?(:noproc)
      refute Shard.retryable_exit?(:killed)
    end

    # The reason this distinction exists at all: a shard that dies *after* handling an out
    # surfaces to the caller carrying the death reason, not :noproc, so it propagates
    # instead of being retried and the tuple is not written twice.
    test "a shard that died mid-call is not retried" do
      refute Shard.retryable_exit?({:killed, {GenServer, :call, [self(), {:out, {:a, 1}}, 5000]}})
    end
  end

  describe "the retry in practice" do
    test "a call to a tag with no shard does not spin", %{} do
      tag = String.to_atom("tuplex_retry_tag_#{System.unique_integer([:positive])}")

      # take/2 resolves nothing and returns the default rather than retrying to a deadline.
      started = System.monotonic_time(:millisecond)
      assert :empty = Shard.take({tag, :_})
      assert System.monotonic_time(:millisecond) - started < 500
    end

    test "an out survives its shard being replaced underneath it" do
      tag = String.to_atom("tuplex_retry_tag_#{System.unique_integer([:positive])}")
      {:ok, pid} = Shard.ensure(tag)

      ref = Process.monitor(pid)
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^pid, :killed}

      # The registry may still name the corpse; the call waits for the replacement rather
      # than exiting with :noproc.
      assert :ok = Tuplex.out({tag, 1})
      assert [{^tag, 1}] = Tuplex.rd_all({tag, :_})
    end
  end
end
