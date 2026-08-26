defmodule Tuplex.StoreTest do
  use ExUnit.Case, async: true

  alias Tuplex.Store
  alias Tuplex.Template

  doctest Tuplex.Store

  setup do
    tab = Store.new(:store_test)
    on_exit(fn -> if :ets.info(tab) != :undefined, do: Store.destroy(tab) end)
    {:ok, tab: tab, seq: counter()}
  end

  describe "insert/3" do
    test "writes a tuple", %{tab: tab, seq: seq} do
      assert :ok = Store.insert(tab, seq.(), {:job, 1})
      assert 1 = Store.size(tab)
    end

    test "accepts tuples a template could never contain", %{tab: tab, seq: seq} do
      assert :ok = Store.insert(tab, seq.(), {:cfg, :_, :"$1", %{a: :_}})
      assert [{:cfg, :_, :"$1", %{a: :_}}] = Store.read_all(tab, {:cfg, :_, :_, :_})
    end

    test "raises rather than silently overwriting a reused sequence", %{tab: tab} do
      assert :ok = Store.insert(tab, 1, {:job, 1})

      assert_raise ArgumentError, ~r/already present/, fn ->
        Store.insert(tab, 1, {:job, 2})
      end

      assert 1 = Store.size(tab)
    end
  end

  # The reason the storage form is an ordered_set keyed by sequence rather than a
  # duplicate_bag keyed by tag. In a duplicate_bag there is no primitive that removes one of
  # N identical rows: delete_object/2 removes every equal object and take/2 removes the
  # whole key. Both would drain a semaphore in a single `in`.
  describe "take/2 with identical tuples" do
    setup %{tab: tab, seq: seq} do
      for _ <- 1..3, do: Store.insert(tab, seq.(), {:token})
      :ok
    end

    test "removes exactly one copy", %{tab: tab} do
      assert 3 = Store.size(tab)
      assert {:ok, {:token}} = Store.take(tab, {:token})
      assert 2 = Store.size(tab)
    end

    test "the remaining copies are still readable", %{tab: tab} do
      assert {:ok, {:token}} = Store.take(tab, {:token})
      assert {:ok, {:token}} = Store.read(tab, {:token})
      assert [{:token}, {:token}] = Store.read_all(tab, {:token})
    end

    test "drains one at a time and then reports empty", %{tab: tab} do
      assert {:ok, {:token}} = Store.take(tab, {:token})
      assert {:ok, {:token}} = Store.take(tab, {:token})
      assert {:ok, {:token}} = Store.take(tab, {:token})
      assert :empty = Store.take(tab, {:token})
      assert 0 = Store.size(tab)
    end

    test "identical tuples count separately", %{tab: tab, seq: seq} do
      Store.insert(tab, seq.(), {:token})
      assert 4 = Store.size(tab)
      assert length(Store.read_all(tab, {:token})) == 4
    end
  end

  describe "take/2" do
    test "returns :empty when the space holds nothing matching", %{tab: tab, seq: seq} do
      assert :empty = Store.take(tab, {:job, :_})
      Store.insert(tab, seq.(), {:task, 1})
      assert :empty = Store.take(tab, {:job, :_})
      assert 1 = Store.size(tab)
    end

    test "takes the oldest match first", %{tab: tab, seq: seq} do
      for n <- 1..3, do: Store.insert(tab, seq.(), {:job, n})

      assert {:ok, {:job, 1}} = Store.take(tab, {:job, :_})
      assert {:ok, {:job, 2}} = Store.take(tab, {:job, :_})
      assert {:ok, {:job, 3}} = Store.take(tab, {:job, :_})
    end

    test "oldest-first is by sequence, not by insertion call order", %{tab: tab} do
      Store.insert(tab, 20, {:job, :second})
      Store.insert(tab, 10, {:job, :first})

      assert {:ok, {:job, :first}} = Store.take(tab, {:job, :_})
    end

    test "skips non-matching tuples to reach an older match", %{tab: tab, seq: seq} do
      Store.insert(tab, seq.(), {:task, 1})
      Store.insert(tab, seq.(), {:job, 1})
      Store.insert(tab, seq.(), {:task, 2})

      assert {:ok, {:job, 1}} = Store.take(tab, {:job, :_})
      assert 2 = Store.size(tab)
    end
  end

  describe "read/2 and read_all/2" do
    setup %{tab: tab, seq: seq} do
      for n <- 1..3, do: Store.insert(tab, seq.(), {:job, n})
      :ok
    end

    test "read returns the oldest match and leaves it in place", %{tab: tab} do
      assert {:ok, {:job, 1}} = Store.read(tab, {:job, :_})
      assert {:ok, {:job, 1}} = Store.read(tab, {:job, :_})
      assert 3 = Store.size(tab)
    end

    test "read returns :empty with nothing matching", %{tab: tab} do
      assert :empty = Store.read(tab, {:nope, :_})
    end

    test "read_all returns every match, oldest first", %{tab: tab} do
      assert [{:job, 1}, {:job, 2}, {:job, 3}] = Store.read_all(tab, {:job, :_})
      assert 3 = Store.size(tab)
    end

    test "read_all returns [] with nothing matching", %{tab: tab} do
      assert [] = Store.read_all(tab, {:nope, :_})
    end

    test "read_all narrows on a literal", %{tab: tab} do
      assert [{:job, 2}] = Store.read_all(tab, {:job, 2})
    end
  end

  describe "matching" do
    test "the tag is part of the match", %{tab: tab, seq: seq} do
      Store.insert(tab, seq.(), {:job, 1})
      assert :empty = Store.read(tab, {:task, :_})
    end

    test "arity is part of the match", %{tab: tab, seq: seq} do
      Store.insert(tab, seq.(), {:job, 1, 2})

      assert :empty = Store.read(tab, {:job, :_})
      assert :empty = Store.read(tab, {:job, :_, :_, :_})
      assert {:ok, {:job, 1, 2}} = Store.read(tab, {:job, :_, :_})
    end

    test "a float does not match a stored integer", %{tab: tab, seq: seq} do
      Store.insert(tab, seq.(), {:n, 1})

      assert :empty = Store.read(tab, {:n, 1.0})
      assert {:ok, {:n, 1}} = Store.read(tab, {:n, 1})
    end

    test "wildcards match nested inside tuples and lists", %{tab: tab, seq: seq} do
      Store.insert(tab, seq.(), {:point, {:x, 9}})
      Store.insert(tab, seq.(), {:list, [1, 2, 3]})

      assert {:ok, {:point, {:x, 9}}} = Store.read(tab, {:point, {:x, :_}})
      assert :empty = Store.read(tab, {:point, {:y, :_}})
      assert {:ok, {:list, [1, 2, 3]}} = Store.read(tab, {:list, [1, :_, 3]})
      assert :empty = Store.read(tab, {:list, [1, :_, 4]})
    end
  end

  # Maps are hoisted out of the head into =:= guards precisely so this stays true. Left in
  # the head, ETS would match them partially and %{a: 1} would find %{a: 1, b: 2}.
  describe "matching maps" do
    setup %{tab: tab, seq: seq} do
      Store.insert(tab, seq.(), {:cfg, %{region: :north}})
      Store.insert(tab, seq.(), {:cfg, %{region: :north, tier: 2}})
      :ok
    end

    test "a map template matches only an equal map", %{tab: tab} do
      assert [{:cfg, %{region: :north}}] = Store.read_all(tab, {:cfg, %{region: :north}})
    end

    test "a map template does not match a superset", %{tab: tab} do
      assert [] = Store.read_all(tab, {:cfg, %{tier: 2}})
      assert [] = Store.read_all(tab, {:cfg, %{}})
    end

    test "a map template does not match a subset", %{tab: tab} do
      assert [{:cfg, %{region: :north, tier: 2}}] =
               Store.read_all(tab, {:cfg, %{region: :north, tier: 2}})
    end

    test "map values are matched exactly", %{tab: tab, seq: seq} do
      Store.insert(tab, seq.(), {:cfg, %{n: 1}})

      assert [] = Store.read_all(tab, {:cfg, %{n: 1.0}})
      assert [{:cfg, %{n: 1}}] = Store.read_all(tab, {:cfg, %{n: 1}})
    end

    test "a wildcard beside a map still works", %{tab: tab, seq: seq} do
      Store.insert(tab, seq.(), {:job, 7, %{region: :north}})

      assert {:ok, {:job, 7, %{region: :north}}} =
               Store.read(tab, {:job, :_, %{region: :north}})

      assert :empty = Store.read(tab, {:job, :_, %{region: :south}})
    end

    test "a hoisted subterm carrying a map is matched whole", %{tab: tab, seq: seq} do
      Store.insert(tab, seq.(), {:job, {:meta, %{a: 1}, 2}})

      assert {:ok, _} = Store.read(tab, {:job, {:meta, %{a: 1}, 2}})
      assert :empty = Store.read(tab, {:job, {:meta, %{a: 1}, 3}})
      assert :empty = Store.read(tab, {:job, {:meta, %{a: 1, b: 2}, 2}})
    end

    test "take removes a map-matched tuple", %{tab: tab} do
      assert {:ok, {:cfg, %{region: :north}}} = Store.take(tab, {:cfg, %{region: :north}})
      assert [{:cfg, %{region: :north, tier: 2}}] = Store.read_all(tab, {:cfg, :_})
    end
  end

  describe "delete/2" do
    test "removes the row written under a sequence", %{tab: tab} do
      Store.insert(tab, 1, {:job, 1})
      Store.insert(tab, 2, {:job, 2})

      assert :ok = Store.delete(tab, 1)
      assert [{:job, 2}] = Store.read_all(tab, {:job, :_})
    end

    test "is fine with a sequence already gone", %{tab: tab} do
      Store.insert(tab, 1, {:job, 1})
      assert {:ok, _} = Store.take(tab, {:job, :_})
      assert :ok = Store.delete(tab, 1)
    end

    test "removes one copy of identical tuples", %{tab: tab} do
      Store.insert(tab, 1, {:token})
      Store.insert(tab, 2, {:token})

      assert :ok = Store.delete(tab, 1)
      assert [{:token}] = Store.read_all(tab, {:token})
    end
  end

  describe "size/1 and to_list/1" do
    test "size counts rows, not distinct tuples", %{tab: tab, seq: seq} do
      for _ <- 1..3, do: Store.insert(tab, seq.(), {:token})
      assert 3 = Store.size(tab)
    end

    test "to_list returns rows oldest first", %{tab: tab} do
      Store.insert(tab, 2, {:job, :second})
      Store.insert(tab, 1, {:job, :first})

      assert [{1, {:job, :first}}, {2, {:job, :second}}] = Store.to_list(tab)
    end
  end

  # Store's match specs and Template.matches?/2 must express the same relation. Blocking
  # reads test freshly written tuples against waiting templates with the pure matcher, while
  # table reads go through the spec — if the two drift, the waiter index and the table
  # disagree about what is in the space.
  describe "read_all/2 agrees with Template.matches?/2" do
    @corpus [
      {:job, 1, "a"},
      {:job, 1, "b"},
      {:job, 2, "a"},
      {:job, 1.0, "a"},
      {:job, 1, "a"},
      {:job, 1},
      {:job, 1, 2, 3},
      {:point, {:x, 1}},
      {:point, {:x, 2}},
      {:point, {:y, 1}},
      {:point, {:x, 1, 2}},
      {:list, [1, 2, 3]},
      {:list, [1, :b, 3]},
      {:list, [1, 2]},
      {:list, []},
      {:list, [1 | :tail]},
      {:ping},
      {:flag, true},
      {:flag, false},
      {:s, "ab"},
      {:s, ~c"ab"},
      {:bin, <<1, 2>>},
      {:bin, <<1, 2, 3>>},
      {:mixed, :_, 1},
      {:mixed, :other, 1},
      {:cfg, %{a: 1}},
      {:cfg, %{a: 1, b: 2}},
      {:cfg, %{a: 1.0}},
      {:cfg, %{}},
      {:cfg, :_, %{a: 1}},
      {:deep, {:meta, %{a: 1}, 2}},
      {:deep, {:meta, %{a: 1}, 3}},
      {:inlist, [1, %{a: 1}]}
    ]

    @templates [
      {:job, :_, :_},
      {:job, 1, :_},
      {:job, 1.0, :_},
      {:job, :_, "a"},
      {:job, 1, "a"},
      {:job, :_},
      {:job, :_, :_, :_},
      {:point, {:x, :_}},
      {:point, :_},
      {:point, {:_, 1}},
      {:list, [1, :_, 3]},
      {:list, [1, 2]},
      {:list, []},
      {:list, :_},
      {:list, [1 | :_]},
      {:ping},
      {:flag, true},
      {:flag, :_},
      {:s, "ab"},
      {:s, ~c"ab"},
      {:bin, <<1, 2>>},
      {:mixed, :_, :_},
      {:mixed, :_, 1},
      {:cfg, :_},
      {:cfg, %{a: 1}},
      {:cfg, %{a: 1.0}},
      {:cfg, %{a: 1, b: 2}},
      {:cfg, %{}},
      {:cfg, :_, %{a: 1}},
      {:deep, {:meta, %{a: 1}, 2}},
      {:deep, :_},
      {:inlist, [1, %{a: 1}]},
      {:inlist, [:_, %{a: 1}]},
      {:absent, :_}
    ]

    setup %{tab: tab, seq: seq} do
      for tuple <- @corpus, do: Store.insert(tab, seq.(), tuple)
      :ok
    end

    for template <- @templates do
      test "#{inspect(template)}", %{tab: tab} do
        template = unquote(Macro.escape(template))

        assert {:ok, ^template} = Template.validate(template)

        # read_all is oldest first and the corpus was inserted in order, so the two lists
        # must agree on ordering and on duplicates, not merely as sets.
        assert Store.read_all(tab, template) ==
                 Enum.filter(@corpus, &Template.matches?(template, &1))
      end
    end
  end

  defp counter do
    {:ok, agent} = Agent.start_link(fn -> 0 end)
    fn -> Agent.get_and_update(agent, fn n -> {n + 1, n + 1} end) end
  end
end
