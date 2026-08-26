defmodule Tuplex.TemplateTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

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

    test "rejects $-prefixed atoms that would land in the head" do
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

    test "allows reserved atoms inside a hoisted subterm, where ETS never reads them" do
      assert {:ok, _} = Template.validate({:cfg, %{name: :"$1"}})
      assert {:ok, _} = Template.validate({:cfg, {:"$1", %{a: 1}}})
    end

    test "atoms that merely contain a dollar sign are fine" do
      assert {:ok, _} = Template.validate({:job, :"a$b"})
      assert {:ok, _} = Template.validate({:job, :"_$"})
    end
  end

  describe "validate/1 with maps" do
    test "accepts maps, which are ordinary terms" do
      assert {:ok, _} = Template.validate({:job, %{region: :north}})
      assert {:ok, _} = Template.validate({:job, :_, %{region: :north}})
      assert {:ok, _} = Template.validate({:job, %{}})
      assert {:ok, _} = Template.validate({:job, %URI{}})
    end

    test "accepts maps nested inside tuples and lists" do
      assert {:ok, _} = Template.validate({:job, {:inner, %{a: 1}}})
      assert {:ok, _} = Template.validate({:job, [%{a: 1}, %{b: 2}]})
      assert {:ok, _} = Template.validate({:job, %{outer: %{inner: 1}}})
    end

    test "rejects a wildcard inside a map" do
      assert {:error, {:wildcard_in_map, %{a: :_}}} = Template.validate({:job, %{a: :_}})
    end

    test "rejects a wildcard nested deeper inside a map" do
      assert {:error, {:wildcard_in_map, %{a: {1, :_}}}} =
               Template.validate({:job, %{a: {1, :_}}})

      assert {:error, {:wildcard_in_map, %{a: [1, :_]}}} =
               Template.validate({:job, %{a: [1, :_]}})
    end

    test "rejects a wildcard used as a map key" do
      map = %{:_ => 1}
      assert {:error, {:wildcard_in_map, ^map}} = Template.validate({:job, map})
    end

    test "rejects a wildcard in a map nested inside other structure" do
      assert {:error, {:wildcard_in_map, %{a: :_}}} =
               Template.validate({:job, {:inner, %{a: :_}}})

      assert {:error, {:wildcard_in_map, %{a: :_}}} = Template.validate({:job, [%{a: :_}]})
    end

    test "a wildcard beside a map is fine — only inside one is a problem" do
      assert {:ok, _} = Template.validate({:job, :_, %{a: 1}})
      assert {:ok, _} = Template.validate({:job, {:_, %{a: 1}}})
      assert {:ok, _} = Template.validate({:job, [:_, %{a: 1}]})
    end
  end

  describe "compile/1" do
    test "a map-free template compiles to itself with no guards" do
      assert {:ok, {{:job, :_, 2}, []}} = Template.compile({:job, :_, 2})
      assert {:ok, {{:ping}, []}} = Template.compile({:ping})
    end

    test "hoists a map into an equality guard" do
      assert {:ok, {{:job, :_, :"$1"}, [{:"=:=", :"$1", {:const, %{region: :north}}}]}} =
               Template.compile({:job, :_, %{region: :north}})
    end

    test "hoists the largest wildcard-free subterm, not just the map" do
      assert {:ok, {{:job, :"$1"}, [{:"=:=", :"$1", {:const, {:meta, %{a: 1}, 2}}}]}} =
               Template.compile({:job, {:meta, %{a: 1}, 2}})
    end

    test "descends past a wildcard to reach the map beside it" do
      assert {:ok, {{:job, {:_, :"$1"}}, [{:"=:=", :"$1", {:const, %{a: 1}}}]}} =
               Template.compile({:job, {:_, %{a: 1}}})
    end

    test "numbers several hoisted subterms in order" do
      assert {:ok, {{:job, :"$1", :_, :"$2"}, guards}} =
               Template.compile({:job, %{a: 1}, :_, %{b: 2}})

      assert [{:"=:=", :"$1", {:const, %{a: 1}}}, {:"=:=", :"$2", {:const, %{b: 2}}}] = guards
    end

    test "keeps tag and arity in the head even when everything else is hoisted" do
      assert {:ok, {{:job, :"$1"}, _}} = Template.compile({:job, %{a: 1}})
    end

    test "hoists a map inside a list" do
      assert {:ok, {{:job, [1, :_, :"$1"]}, [{:"=:=", :"$1", {:const, %{a: 1}}}]}} =
               Template.compile({:job, [1, :_, %{a: 1}]})
    end

    test "hoists a whole map-bearing list when nothing in it is wildcarded" do
      assert {:ok, {{:job, :"$1"}, [{:"=:=", :"$1", {:const, [1 | %{a: 1}]}}]}} =
               Template.compile({:job, [1 | %{a: 1}]})
    end

    test "hoists a map in an improper list tail when the list is wildcarded" do
      assert {:ok, {{:job, [:_ | :"$1"]}, [{:"=:=", :"$1", {:const, %{a: 1}}}]}} =
               Template.compile({:job, [:_ | %{a: 1}]})
    end

    test "propagates validation errors" do
      assert {:error, :wildcard_tag} = Template.compile({:_, 1})
      assert {:error, {:wildcard_in_map, %{a: :_}}} = Template.compile({:job, %{a: :_}})
    end
  end

  describe "compile!/1 and validate!/1" do
    test "return on success" do
      assert {{:job, :_}, []} = Template.compile!({:job, :_})
      assert {:job, :_} = Template.validate!({:job, :_})
    end

    test "raise with an explanation" do
      assert_raise ArgumentError, ~r/the tag may not be the wildcard/, fn ->
        Template.validate!({:_, 1})
      end

      assert_raise ArgumentError, ~r/reserved.*match variables/, fn ->
        Template.validate!({:job, :"$1"})
      end

      assert_raise ArgumentError, ~r/wildcards inside maps/, fn ->
        Template.compile!({:job, %{a: :_}})
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
      assert {:ok, _} = Template.validate_tuple({:cfg, %{a: :_}})
      assert {:ok, _} = Template.validate_tuple({:cfg, :"$1"})
      assert {:ok, _} = Template.validate_tuple({:cfg, :_})
      assert {:ok, _} = Template.validate_tuple({:cfg, [%{a: 1}, :"$99"]})
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

    test "maps match by equality, never as a subset" do
      assert Template.matches?({:cfg, %{a: 1}}, {:cfg, %{a: 1}})
      refute Template.matches?({:cfg, %{a: 1}}, {:cfg, %{a: 1, b: 2}})
      refute Template.matches?({:cfg, %{a: 1, b: 2}}, {:cfg, %{a: 1}})
      refute Template.matches?({:cfg, %{a: 1}}, {:cfg, %{a: 1.0}})
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

  # If a template can match a tuple, the two must file under the same key. Shard indexes
  # waiting templates by key and only offers a newly written tuple to the waiters under the
  # tuple's own key — so a disagreement here means `out` looks in the wrong bucket and a
  # legitimately waiting `in` is never woken. That failure is a hang, not a crash, which
  # makes it the worst thing in this codebase to debug and worth a property rather than
  # examples.
  describe "property: matching implies equal keys" do
    property "for templates derived from a tuple by wildcarding" do
      check all(
              tuple <- tuple_gen(),
              template <- wildcarded(tuple)
            ) do
        assert Template.matches?(template, tuple),
               "wildcarding should only ever widen a match"

        assert Template.key(template) == Template.key(tuple)
      end
    end

    property "for independently generated templates and tuples" do
      check all(
              tuple <- tuple_gen(),
              other <- tuple_gen(),
              template <- wildcarded(other)
            ) do
        if Template.matches?(template, tuple) do
          assert Template.key(template) == Template.key(tuple)
        end
      end
    end

    property "a compiled template still agrees with key/1" do
      check all(
              tuple <- tuple_gen(),
              template <- wildcarded(tuple)
            ) do
        assert {:ok, {head, _guards}} = Template.compile(template)
        assert tuple_size(head) == tuple_size(tuple)
        assert elem(head, 0) == elem(tuple, 0)
      end
    end
  end

  # -- generators -------------------------------------------------------------

  defp tag_gen, do: StreamData.member_of([:job, :task, :point, :cfg, :ping])

  defp leaf_gen do
    StreamData.one_of([
      StreamData.integer(),
      StreamData.float(),
      StreamData.binary(),
      StreamData.member_of([:a, :b, true, false, nil, ~c"ab", "ab"])
    ])
  end

  defp term_gen do
    StreamData.tree(leaf_gen(), fn child ->
      StreamData.one_of([
        StreamData.list_of(child, max_length: 3),
        StreamData.map(StreamData.list_of(child, max_length: 3), &List.to_tuple/1),
        StreamData.map(StreamData.list_of(child, max_length: 2), fn terms ->
          Map.new(Enum.with_index(terms), fn {term, index} -> {index, term} end)
        end)
      ])
    end)
  end

  defp tuple_gen do
    StreamData.bind(tag_gen(), fn tag ->
      StreamData.map(StreamData.list_of(term_gen(), max_length: 3), fn terms ->
        List.to_tuple([tag | terms])
      end)
    end)
  end

  # Replaces a random subset of the tuple's top-level elements with the wildcard. The tag is
  # never replaced, since a wildcard tag is not a legal template.
  defp wildcarded(tuple) do
    [tag | terms] = Tuple.to_list(tuple)

    terms
    |> Enum.map(fn term -> StreamData.member_of([term, :_]) end)
    |> StreamData.fixed_list()
    |> StreamData.map(fn chosen -> List.to_tuple([tag | chosen]) end)
  end
end
