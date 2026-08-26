defmodule Tuplex.TemplateTest do
  use ExUnit.Case, async: true

  alias Tuplex.Template

  doctest Tuplex.Template

  describe "validate/1" do
    test "accepts a template of literals" do
      assert {:ok, {:job, 1, "payload"}} = Template.validate({:job, 1, "payload"})
    end

    test "accepts wildcards anywhere but the tag" do
      assert {:ok, _} = Template.validate({:job, :_, :_})
      assert {:ok, _} = Template.validate({:job, :_, 2})
      assert {:ok, _} = Template.validate({:point, {:x, :_}})
      assert {:ok, _} = Template.validate({:list, [1, :_, 3]})
    end

    test "accepts a 1-arity template" do
      assert {:ok, {:ping}} = Template.validate({:ping})
    end

    test "rejects a wildcard tag" do
      assert {:error, :wildcard_tag} = Template.validate({:_, 1})
      assert {:error, :wildcard_tag} = Template.validate({:_})
    end

    test "rejects a non-atom tag" do
      assert {:error, {:non_atom_tag, "job"}} = Template.validate({"job", 1})
      assert {:error, {:non_atom_tag, 1}} = Template.validate({1, :_})
      assert {:error, {:non_atom_tag, {:nested, :tag}}} = Template.validate({{:nested, :tag}, 1})
    end

    test "rejects non-tuples" do
      assert {:error, :not_a_tuple} = Template.validate([:job, 1])
      assert {:error, :not_a_tuple} = Template.validate(:job)
      assert {:error, :not_a_tuple} = Template.validate("job")
    end

    test "rejects the empty tuple" do
      assert {:error, :empty_tuple} = Template.validate({})
    end

    test "rejects $-prefixed atoms, which ETS would read as match variables" do
      assert {:error, {:reserved_atom, :"$1"}} = Template.validate({:job, :"$1"})
      assert {:error, {:reserved_atom, :"$_"}} = Template.validate({:job, :"$_"})
      assert {:error, {:reserved_atom, :"$$"}} = Template.validate({:job, :"$$"})
      assert {:error, {:reserved_atom, :"$anything"}} = Template.validate({:job, :"$anything"})
      assert {:error, {:reserved_atom, :"$1"}} = Template.validate({:"$1", 1})
    end

    test "finds reserved atoms nested inside tuples and lists" do
      assert {:error, {:reserved_atom, :"$1"}} = Template.validate({:job, {:inner, :"$1"}})
      assert {:error, {:reserved_atom, :"$1"}} = Template.validate({:job, [1, :"$1"]})
      assert {:error, {:reserved_atom, :"$1"}} = Template.validate({:job, [1, [{:deep, :"$1"}]]})
    end

    test "finds a reserved atom in an improper list tail" do
      assert {:error, {:reserved_atom, :"$1"}} = Template.validate({:job, [1 | :"$1"]})
    end

    test "rejects maps, which ETS matches only partially" do
      assert {:error, {:map_in_template, %{a: 1}}} = Template.validate({:job, %{a: 1}})
      assert {:error, {:map_in_template, %{}}} = Template.validate({:job, %{}})
    end

    test "finds maps nested inside tuples and lists" do
      assert {:error, {:map_in_template, %{a: 1}}} = Template.validate({:job, {:inner, %{a: 1}}})
      assert {:error, {:map_in_template, %{a: 1}}} = Template.validate({:job, [%{a: 1}]})
    end

    test "a struct is a map and is rejected too" do
      assert {:error, {:map_in_template, %URI{}}} = Template.validate({:job, %URI{}})
    end

    test "atoms that merely contain a dollar sign are fine" do
      assert {:ok, _} = Template.validate({:job, :"a$b"})
      assert {:ok, _} = Template.validate({:job, :"_$"})
    end
  end

  describe "validate!/1" do
    test "returns the template when valid" do
      assert {:job, :_} = Template.validate!({:job, :_})
    end

    test "raises with an explanation when invalid" do
      assert_raise ArgumentError, ~r/the tag may not be the wildcard/, fn ->
        Template.validate!({:_, 1})
      end

      assert_raise ArgumentError, ~r/reserved.*match variables/, fn ->
        Template.validate!({:job, :"$1"})
      end

      assert_raise ArgumentError, ~r/maps are not supported/, fn ->
        Template.validate!({:job, %{a: 1}})
      end
    end
  end

  describe "validate_tuple/1" do
    test "accepts ordinary tuples" do
      assert {:ok, {:job, 1, "payload"}} = Template.validate_tuple({:job, 1, "payload"})
      assert {:ok, {:ping}} = Template.validate_tuple({:ping})
    end

    test "enforces the same tag rules as templates" do
      assert {:error, :not_a_tuple} = Template.validate_tuple([:job])
      assert {:error, :empty_tuple} = Template.validate_tuple({})
      assert {:error, :wildcard_tag} = Template.validate_tuple({:_, 1})
      assert {:error, {:non_atom_tag, "job"}} = Template.validate_tuple({"job", 1})
      assert {:error, {:reserved_atom, :"$1"}} = Template.validate_tuple({:"$1", 1})
    end

    test "stored content is data, so terms illegal in a template are allowed" do
      assert {:ok, _} = Template.validate_tuple({:cfg, %{retries: 3}})
      assert {:ok, _} = Template.validate_tuple({:cfg, :"$1"})
      assert {:ok, _} = Template.validate_tuple({:cfg, :_})
      assert {:ok, _} = Template.validate_tuple({:cfg, [%{a: 1}, :"$99"]})
      assert {:ok, _} = Template.validate_tuple({:cfg, %URI{}})
    end
  end

  describe "key/1" do
    test "pairs the tag with the arity" do
      assert {:job, 3} = Template.key({:job, 1, "payload"})
      assert {:job, 3} = Template.key({:job, :_, :_})
      assert {:ping, 1} = Template.key({:ping})
    end

    test "a tuple and a template of the same shape share a key" do
      assert Template.key({:job, 1, "payload"}) == Template.key({:job, :_, :_})
    end

    test "differing arity gives a differing key" do
      refute Template.key({:job, 1}) == Template.key({:job, 1, 2})
    end
  end

  describe "match_spec/1" do
    test "wraps the template as a nested head pattern and returns the whole record" do
      assert [{{{:job, 3}, :_, {:job, :_, 2}}, [], [:"$_"]}] =
               Template.match_spec({:job, :_, 2})
    end

    test "is accepted by ETS" do
      tab = new_table()
      assert [] = :ets.select(tab, Template.match_spec({:job, :_, :_}))
    end
  end

  describe "matches?/2" do
    test "a literal template matches only an identical tuple" do
      assert Template.matches?({:job, 1, "a"}, {:job, 1, "a"})
      refute Template.matches?({:job, 1, "a"}, {:job, 1, "b"})
    end

    test "the wildcard matches any single element" do
      assert Template.matches?({:job, :_, "a"}, {:job, 1, "a"})
      assert Template.matches?({:job, :_, "a"}, {:job, %{deep: [1]}, "a"})
      assert Template.matches?({:job, :_, :_}, {:job, 1, "a"})
    end

    test "the wildcard matches nested inside tuples and lists" do
      assert Template.matches?({:point, {:x, :_}}, {:point, {:x, 9}})
      refute Template.matches?({:point, {:x, :_}}, {:point, {:y, 9}})
      assert Template.matches?({:list, [1, :_, 3]}, {:list, [1, 2, 3]})
      refute Template.matches?({:list, [1, :_, 3]}, {:list, [1, 2, 4]})
    end

    test "arity is part of the match" do
      refute Template.matches?({:job, :_}, {:job, 1, 2})
      refute Template.matches?({:job, :_, :_, :_}, {:job, 1, 2})
      refute Template.matches?({:point, {:x, :_}}, {:point, {:x, 1, 2}})
    end

    test "the tag is part of the match" do
      refute Template.matches?({:job, :_}, {:task, 1})
    end

    test "matching is exact: a float does not match an integer" do
      refute Template.matches?({:n, 1.0}, {:n, 1})
      refute Template.matches?({:n, 1}, {:n, 1.0})
      assert Template.matches?({:n, 1.0}, {:n, 1.0})
    end

    test "lists of differing length do not match" do
      refute Template.matches?({:list, [1, 2]}, {:list, [1, 2, 3]})
      refute Template.matches?({:list, [1, 2, 3]}, {:list, [1, 2]})
      assert Template.matches?({:list, []}, {:list, []})
    end

    test "a charlist does not match a binary" do
      refute Template.matches?({:s, ~c"ab"}, {:s, "ab"})
      assert Template.matches?({:s, "ab"}, {:s, "ab"})
    end

    test "improper lists match structurally" do
      assert Template.matches?({:l, [1 | :tail]}, {:l, [1 | :tail]})
      assert Template.matches?({:l, [1 | :_]}, {:l, [1 | :tail]})
      refute Template.matches?({:l, [1 | :tail]}, {:l, [1, :tail]})
    end

    test "a stored :_ is data and is matched literally" do
      assert Template.matches?({:mixed, :_}, {:mixed, :_})
      assert Template.matches?({:mixed, :_}, {:mixed, 1})
    end
  end

  # The pure matcher and the ETS match spec must express exactly the same relation. If they
  # ever drift, blocking reads (which use the pure matcher against waiting templates) would
  # disagree with table reads about what is in the space.
  describe "matches?/2 agrees with the ETS match spec" do
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
      {:cfg, %{a: 1, b: 2}}
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
      {:absent, :_}
    ]

    setup do
      tab = new_table()

      for tuple <- @corpus do
        :ets.insert(tab, {Template.key(tuple), :erlang.unique_integer([:monotonic]), tuple})
      end

      {:ok, tab: tab}
    end

    for template <- @templates do
      test "#{inspect(template)}", %{tab: tab} do
        template = unquote(Macro.escape(template))

        assert {:ok, ^template} = Template.validate(template)

        from_ets =
          tab
          |> :ets.select(Template.match_spec(template))
          |> Enum.map(&elem(&1, 2))
          |> Enum.sort()

        from_pure =
          @corpus
          |> Enum.filter(&Template.matches?(template, &1))
          |> Enum.sort()

        assert from_ets == from_pure
      end
    end

    test "duplicates are preserved on both sides", %{tab: tab} do
      # {:job, 1, "a"} appears twice in the corpus.
      from_ets = :ets.select(tab, Template.match_spec({:job, 1, "a"}))
      from_pure = Enum.filter(@corpus, &Template.matches?({:job, 1, "a"}, &1))

      assert length(from_ets) == 2
      assert length(from_pure) == 2
    end
  end

  defp new_table do
    tab = :ets.new(:tuplex_template_test, [:duplicate_bag, :public])
    on_exit(fn -> if :ets.info(tab) != :undefined, do: :ets.delete(tab) end)
    tab
  end
end
