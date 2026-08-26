defmodule Tuplex.StatemTest do
  use ExUnit.Case, async: true
  use PropCheck
  use PropCheck.StateM

  alias Tuplex.Shard
  alias Tuplex.Template

  # A stateful model of the **deterministic serial subset** of the API: out, inp, rdp,
  # rd_all, tags. The model is a map of tag to an ordered list of tuples, oldest first, and
  # that is enough to make every postcondition exact rather than a range — including *which*
  # tuple comes back, since FIFO means `inp` returns the oldest match and not an arbitrary
  # one. That pins the ordering guarantee that fell out of `:ordered_set` in step 1 and has
  # been relied on ever since.
  #
  # Blocking `in`/`rd`, leases, kills and watches are deliberately **out of the model**.
  # Blocking operations do not return, so making them commands would mean contorting the
  # model into something no longer trustworthy, and a subtly wrong model produces false
  # failures that cost a day each. Those claims are defended by the hand-written properties,
  # which can orchestrate processes in ways a statem model cannot express.
  #
  # What this buys over the hand-written properties is not coverage but **shrinking**: a
  # failing forty-command sequence comes back as the three-command minimum, where a
  # hand-rolled concurrency test hands you a wall.

  @tags [:a, :b]
  @values [1, 2]

  property "the serial API matches a list-of-tuples model" do
    numtests(
      runs(),
      forall cmds <- commands(__MODULE__) do
        namespace = fresh_namespace()

        try do
          {history, state, result} = run_commands(__MODULE__, cmds)

          (result == :ok)
          |> when_fail(
            IO.puts("""

            History: #{inspect(history, pretty: true)}
            Model:   #{inspect(state, pretty: true)}
            Result:  #{inspect(result, pretty: true)}
            """)
          )
          |> aggregate(command_names(cmds))
        after
          cleanup(namespace)
        end
      end
    )
  end

  # -- the model --------------------------------------------------------------

  @impl true
  def initial_state, do: %{}

  @impl true
  def command(_state) do
    oneof([
      {:call, __MODULE__, :out, [tuple_gen()]},
      {:call, __MODULE__, :inp, [template_gen()]},
      {:call, __MODULE__, :rdp, [template_gen()]},
      {:call, __MODULE__, :rd_all, [template_gen()]},
      {:call, __MODULE__, :tags, []}
    ])
  end

  @impl true
  def precondition(_state, _call), do: true

  @impl true
  # A tag gains an entry on its first write, and keeps it once emptied: that is exactly when
  # a shard exists, which is what tags/0 reports.
  def next_state(state, _result, {:call, _mod, :out, [tuple]}) do
    Map.update(state, elem(tuple, 0), [tuple], &(&1 ++ [tuple]))
  end

  def next_state(state, _result, {:call, _mod, :inp, [template]}) do
    case Map.fetch(state, elem(template, 0)) do
      {:ok, tuples} -> Map.put(state, elem(template, 0), drop_first_match(tuples, template))
      :error -> state
    end
  end

  def next_state(state, _result, _call), do: state

  @impl true
  def postcondition(_state, {:call, _mod, :out, [_tuple]}, result) do
    result == :ok
  end

  # Exact, not "some matching tuple": FIFO means the oldest match and no other.
  def postcondition(state, {:call, _mod, :inp, [template]}, result) do
    result == expected_one(state, template)
  end

  def postcondition(state, {:call, _mod, :rdp, [template]}, result) do
    result == expected_one(state, template)
  end

  # Every match, oldest first, duplicates included.
  def postcondition(state, {:call, _mod, :rd_all, [template]}, result) do
    result == matches(state, template)
  end

  # tags/0 reports every live shard on the node, so other tests' tags are legitimately in
  # there too. The exact half of the claim is the half that matters: nothing we have written
  # to is ever missing.
  def postcondition(state, {:call, _mod, :tags, []}, result) do
    Enum.all?(Map.keys(state), &(realise_tag(&1) in result))
  end

  defp expected_one(state, template) do
    case matches(state, template) do
      [] -> :empty
      [oldest | _rest] -> {:ok, oldest}
    end
  end

  defp matches(state, template) do
    state
    |> Map.get(elem(template, 0), [])
    |> Enum.filter(&Template.matches?(template, &1))
  end

  defp drop_first_match(tuples, template) do
    case Enum.split_while(tuples, &(not Template.matches?(template, &1))) do
      {before, [_match | rest]} -> before ++ rest
      {all, []} -> all
    end
  end

  # -- the adapter ------------------------------------------------------------
  #
  # The model speaks in abstract tags so that a generated command sequence is portable
  # between runs; the adapter maps them onto a namespace unique to the run, so no two runs
  # share a space and nothing has to be torn down mid-sequence.

  def out(tuple), do: Tuplex.out(realise(tuple))
  def inp(template), do: abstract(Tuplex.inp(realise(template)))
  def rdp(template), do: abstract(Tuplex.rdp(realise(template)))
  def rd_all(template), do: Enum.map(Tuplex.rd_all(realise(template)), &abstract_tuple/1)
  def tags, do: Tuplex.tags()

  defp realise(tuple), do: put_elem(tuple, 0, realise_tag(elem(tuple, 0)))

  defp realise_tag(tag), do: :"tuplex_statem_#{tag}_#{Process.get(:tuplex_statem_run)}"

  defp abstract({:ok, tuple}), do: {:ok, abstract_tuple(tuple)}
  defp abstract(:empty), do: :empty

  defp abstract_tuple(tuple) do
    put_elem(tuple, 0, tuple |> elem(0) |> Atom.to_string() |> abstract_tag())
  end

  defp abstract_tag("tuplex_statem_" <> rest) do
    rest |> String.split("_") |> hd() |> String.to_existing_atom()
  end

  defp fresh_namespace do
    run = System.unique_integer([:positive])
    Process.put(:tuplex_statem_run, run)
    run
  end

  defp cleanup(run) do
    Process.put(:tuplex_statem_run, run)

    for tag <- @tags do
      case Shard.lookup(realise_tag(tag)) do
        {:ok, pid, _tab} -> DynamicSupervisor.terminate_child(Tuplex.ShardSupervisor, pid)
        :error -> :ok
      end
    end
  end

  # -- generators -------------------------------------------------------------

  defp tuple_gen do
    let [
      tag <- elements(@tags),
      arity <- elements([2, 3]),
      v <- elements(@values),
      w <- elements(@values)
    ] do
      shape(tag, arity, v, w)
    end
  end

  defp template_gen do
    let [
      tag <- elements(@tags),
      arity <- elements([2, 3]),
      v <- elements([:_ | @values]),
      w <- elements([:_ | @values])
    ] do
      shape(tag, arity, v, w)
    end
  end

  # Two arities, so the model exercises the rule that arity is part of the match.
  defp shape(tag, 2, v, _w), do: {tag, v}
  defp shape(tag, 3, v, w), do: {tag, v, w}

  defp runs, do: String.to_integer(System.get_env("TUPLEX_PROP_RUNS", "100"))
end
