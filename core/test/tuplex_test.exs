defmodule TuplexTest do
  use ExUnit.Case, async: true

  doctest Tuplex

  setup do
    {:ok, tag: unique_tag()}
  end

  describe "out/1" do
    test "returns :ok, keeping the sequence number internal", %{tag: tag} do
      assert :ok = Tuplex.out({tag, 1, "payload"})
    end

    test "identical tuples do not collapse", %{tag: tag} do
      assert :ok = Tuplex.out({tag})
      assert :ok = Tuplex.out({tag})
      assert :ok = Tuplex.out({tag})

      assert {:ok, {^tag}} = Tuplex.inp({tag})
      assert {:ok, {^tag}} = Tuplex.inp({tag})
      assert {:ok, {^tag}} = Tuplex.inp({tag})
      assert :empty = Tuplex.inp({tag})
    end

    test "rejects a tuple with no concrete atom tag" do
      assert_raise ArgumentError, ~r/the tag must be an atom/, fn ->
        Tuplex.out({"job", 1})
      end

      assert_raise ArgumentError, ~r/wildcard/, fn -> Tuplex.out({:_, 1}) end
      assert_raise ArgumentError, ~r/expected a tuple/, fn -> Tuplex.out([:job, 1]) end
    end

    test "stores terms that would be illegal in a template", %{tag: tag} do
      assert :ok = Tuplex.out({tag, %{a: :_}, :"$1"})
      assert {:ok, {^tag, %{a: :_}, :"$1"}} = Tuplex.rdp({tag, :_, :_})
    end
  end

  describe "inp/1" do
    test "takes the oldest match and removes it", %{tag: tag} do
      Tuplex.out({tag, 1})
      Tuplex.out({tag, 2})

      assert {:ok, {^tag, 1}} = Tuplex.inp({tag, :_})
      assert {:ok, {^tag, 2}} = Tuplex.inp({tag, :_})
      assert :empty = Tuplex.inp({tag, :_})
    end

    test "returns :empty, not an error tuple, on an empty space", %{tag: tag} do
      assert :empty = Tuplex.inp({tag, :_})
    end

    test "rejects an invalid template", %{tag: tag} do
      assert_raise ArgumentError, ~r/wildcards inside maps/, fn ->
        Tuplex.inp({tag, %{a: :_}})
      end

      assert_raise ArgumentError, ~r/match variables/, fn -> Tuplex.inp({tag, :"$1"}) end
    end
  end

  describe "rdp/1" do
    test "returns the oldest match and leaves it in place", %{tag: tag} do
      Tuplex.out({tag, 1})

      assert {:ok, {^tag, 1}} = Tuplex.rdp({tag, :_})
      assert {:ok, {^tag, 1}} = Tuplex.rdp({tag, :_})
      assert {:ok, {^tag, 1}} = Tuplex.inp({tag, :_})
    end

    test "returns :empty on an empty space", %{tag: tag} do
      assert :empty = Tuplex.rdp({tag, :_})
    end

    test "matches exactly", %{tag: tag} do
      Tuplex.out({tag, 1})

      assert :empty = Tuplex.rdp({tag, 1.0})
      assert :empty = Tuplex.rdp({tag, :_, :_})
      assert {:ok, _} = Tuplex.rdp({tag, 1})
    end

    test "matches maps by equality, not as a subset", %{tag: tag} do
      Tuplex.out({tag, %{region: :north, tier: 2}})

      assert :empty = Tuplex.rdp({tag, %{region: :north}})
      assert {:ok, _} = Tuplex.rdp({tag, %{region: :north, tier: 2}})
    end
  end

  describe "tags/0" do
    test "reports tags that have a shard", %{tag: tag} do
      refute tag in Tuplex.tags()
      Tuplex.out({tag, 1})
      assert tag in Tuplex.tags()
    end

    test "supports an explicit whole-space sweep", %{tag: tag} do
      Tuplex.out({tag, :a, :b})

      found =
        Tuplex.tags()
        |> Enum.flat_map(fn t ->
          case Tuplex.rdp({t, :_, :_}) do
            {:ok, tuple} -> [tuple]
            :empty -> []
          end
        end)

      assert {tag, :a, :b} in found
    end
  end

  defp unique_tag do
    String.to_atom("tuplex_api_tag_#{System.unique_integer([:positive])}")
  end
end
