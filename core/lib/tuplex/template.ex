defmodule Tuplex.Template do
  @moduledoc """
  > #### Internal {: .warning}
  >
  > Published because the matching rules below are the exact semantics of every template in
  > the library, and there is nowhere better to state them. It is **not part of the public
  > API and not covered by semantic versioning**. Use `Tuplex`.

  Validation, shard-key extraction, and match-spec compilation for tuples and templates.

  This module is pure. It performs no side effects, owns no state, and never touches
  `:ets` — it only decides what a template *means*. `Tuplex.Store` turns those decisions
  into table operations, and owns the storage form; nothing here knows how a tuple is
  stored.

  ## Tuples and templates

  A **tuple** is what you write with `Tuplex.out/1`. A **template** is what you match with
  (`in`, `rd`, `inp`, `rdp`, `rd_all`, `watch`). Both are Erlang tuples whose first element
  is an atom **tag**:

      {:job, 1, "payload"}      # a tuple
      {:job, :_, :_}            # a template matching any 3-arity :job tuple

  ## Matching is exact

  Matching uses strict equality, which is the correct Linda semantics: the float `1.0` does
  not match the stored integer `1`. Only `:_` is a wildcard, and it matches at any depth:

      matches?({:point, :_, 2}, {:point, 1, 2})           #=> true
      matches?({:point, {:x, :_}}, {:point, {:x, 9}})     #=> true
      matches?({:n, 1.0}, {:n, 1})                        #=> false

  Arity and tag are part of the match, so an arity-3 template never matches an arity-2
  tuple and a `:job` template never matches a `:task` tuple.

  ## Maps

  Maps are ordinary Elixir terms and templates carry them fine, but they are never placed
  in the ETS head pattern. ETS matches a map in a head *partially* — a `%{a: 1}` pattern
  matches a stored `%{a: 1, b: 2}` — which would be a second, contradictory notion of
  "match" living alongside the exact one above.

  So `compile/1` hoists any map-bearing subterm out of the head and re-attaches it as an
  `=:=` guard:

      compile({:job, :_, %{region: :north}})
      #=> {{:job, :_, :"$1"}, [{:"=:=", :"$1", {:const, %{region: :north}}}]}

  `=:=` is exactly the equality the rest of the module promises: the map must be equal, not
  a subset, and `1.0` still does not match `1`. The hoist takes the largest wildcard-free
  subterm containing the map, so one guard usually covers a whole nested structure, and
  wildcards elsewhere in the template keep working normally.

  The one thing this cannot express is a **wildcard inside a map** — `%{a: :_}`. That would
  need the partial semantics back, so it is rejected. Match the enclosing position with
  `:_` and filter the results yourself.

  ## What a template may not contain

    * **A wildcard tag.** `{:_, :_}` is rejected: the tag selects the shard, so a wildcard
      tag would mean fanning every read across every shard. Use `Tuplex.tags/0` and fold
      over it if you genuinely want that — then the cost is visible at the call site.

    * **`:"$1"`-style atoms in head positions.** ETS reads any atom beginning with `$` as a
      match variable, so `{:x, :"$1"}` would quietly behave as a second wildcard. They are
      fine inside a hoisted subterm, where they end up in a `{:const, _}` guard and are
      never interpreted — `{:cfg, %{name: :"$1"}}` is accepted.

    * **Wildcards inside maps**, as above.

  None of these apply to tuples passed to `out/1`. Stored tuples are data: they are never
  interpreted as patterns, so they may contain `:_`, `:"$1"`, and maps freely. Only the tag
  rule is shared, because storage still has to route them.
  """

  @wildcard :_

  @typedoc "A tuple written into the space with `Tuplex.out/1`."
  @type t :: tuple()

  @typedoc "A pattern matched against stored tuples. `:_` is the wildcard."
  @type template :: tuple()

  @typedoc "The waiter-index key: the tuple's tag paired with its arity."
  @type key :: {atom(), non_neg_integer()}

  @typedoc "An ETS head pattern, shaped like the template it came from."
  @type head :: tuple()

  @typedoc "An ETS guard pinning a hoisted subterm to an exact value."
  @type guard :: {:"=:=", atom(), {:const, term()}}

  @typedoc "Why a tuple or template was rejected."
  @type error ::
          :not_a_tuple
          | :empty_tuple
          | :wildcard_tag
          | {:non_atom_tag, term()}
          | {:reserved_atom, atom()}
          | {:wildcard_in_map, map()}

  @doc """
  Compiles a template into an ETS head pattern and its guards.

  The head is shaped exactly like the template, so tag and arity are matched structurally
  and cheaply. Map-bearing subterms are replaced by `:"$1"`, `:"$2"`, … and pinned by
  `=:=` guards; a template with no maps compiles to itself and no guards at all.

  The caller composes the final match spec, because the record layout belongs to
  `Tuplex.Store`, not here. Store nests this head under its own key position, so the
  variables numbered here can never collide with anything Store introduces.

  ## Examples

      iex> Tuplex.Template.compile({:job, :_, 2})
      {:ok, {{:job, :_, 2}, []}}

      iex> Tuplex.Template.compile({:job, :_, %{region: :north}})
      {:ok, {{:job, :_, :"$1"}, [{:"=:=", :"$1", {:const, %{region: :north}}}]}}

      iex> Tuplex.Template.compile({:job, %{a: :_}})
      {:error, {:wildcard_in_map, %{a: :_}}}
  """
  @spec compile(term()) :: {:ok, {head(), [guard()]}} | {:error, error()}
  def compile(template) do
    with :ok <- check_shape(template),
         :ok <- check_tag(elem(template, 0)) do
      {patterns, _next, guards} = build_terms(Tuple.to_list(template), 1, [])
      {:ok, {List.to_tuple(patterns), :lists.reverse(guards)}}
    end
  catch
    {:invalid, reason} -> {:error, reason}
  end

  @doc """
  Same as `compile/1` but returns the pair or raises `ArgumentError`.
  """
  @spec compile!(term()) :: {head(), [guard()]}
  def compile!(template) do
    case compile(template) do
      {:ok, compiled} -> compiled
      {:error, reason} -> raise ArgumentError, "invalid template: " <> explain(reason)
    end
  end

  @doc """
  Validates a template, returning it unchanged.

  Equivalent to `compile/1` with the compiled form discarded — a template is valid exactly
  when it compiles.

  ## Examples

      iex> Tuplex.Template.validate({:job, :_, 2})
      {:ok, {:job, :_, 2}}

      iex> Tuplex.Template.validate({:job, %{region: :north}})
      {:ok, {:job, %{region: :north}}}

      iex> Tuplex.Template.validate({:_, 1})
      {:error, :wildcard_tag}

      iex> Tuplex.Template.validate({:job, :"$1"})
      {:error, {:reserved_atom, :"$1"}}

      iex> Tuplex.Template.validate([:job])
      {:error, :not_a_tuple}
  """
  @spec validate(term()) :: {:ok, template()} | {:error, error()}
  def validate(template) do
    case compile(template) do
      {:ok, _compiled} -> {:ok, template}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Same as `validate/1` but returns the template or raises `ArgumentError`.
  """
  @spec validate!(term()) :: template()
  def validate!(template) do
    _compiled = compile!(template)
    template
  end

  @doc """
  Validates a tuple destined for the space.

  Only the tag is constrained — it must be a concrete atom, because it selects the shard.
  Everything else is data and is stored verbatim, including terms that would be illegal in
  a template.

  ## Examples

      iex> Tuplex.Template.validate_tuple({:job, 1, "payload"})
      {:ok, {:job, 1, "payload"}}

      iex> Tuplex.Template.validate_tuple({:cfg, %{retries: 3}, :"$1"})
      {:ok, {:cfg, %{retries: 3}, :"$1"}}

      iex> Tuplex.Template.validate_tuple({})
      {:error, :empty_tuple}

      iex> Tuplex.Template.validate_tuple({"job", 1})
      {:error, {:non_atom_tag, "job"}}
  """
  @spec validate_tuple(term()) :: {:ok, t()} | {:error, error()}
  def validate_tuple(tuple) do
    with :ok <- check_shape(tuple),
         :ok <- check_tag(elem(tuple, 0)) do
      {:ok, tuple}
    end
  end

  @doc """
  Same as `validate_tuple/1` but returns the tuple or raises `ArgumentError`.
  """
  @spec validate_tuple!(term()) :: t()
  def validate_tuple!(tuple) do
    case validate_tuple(tuple) do
      {:ok, tuple} -> tuple
      {:error, reason} -> raise ArgumentError, "invalid tuple: " <> explain(reason)
    end
  end

  @doc """
  Returns the waiter-index key for a tuple or template: its tag paired with its arity.

  A tuple and every template that can match it share this key. The shard relies on
  that: a newly written tuple only has to be offered to waiters filed under its own key,
  and if the two ever disagreed a legitimately waiting `in` would never wake.

  ## Examples

      iex> Tuplex.Template.key({:job, 1, "payload"})
      {:job, 3}

      iex> Tuplex.Template.key({:job, :_, :_})
      {:job, 3}

      iex> Tuplex.Template.key({:ping})
      {:ping, 1}
  """
  @spec key(t() | template()) :: key()
  def key(tuple) when is_tuple(tuple) and tuple_size(tuple) > 0 do
    {elem(tuple, 0), tuple_size(tuple)}
  end

  @doc """
  Returns whether `tuple` matches `template`, without consulting ETS.

  This is the same relation the compiled match spec expresses, implemented in plain Elixir
  so a shard can test a freshly written tuple against waiting templates without a table
  round-trip.

  ## Examples

      iex> Tuplex.Template.matches?({:job, :_, 2}, {:job, 1, 2})
      true

      iex> Tuplex.Template.matches?({:job, :_}, {:job, 1, 2})
      false

      iex> Tuplex.Template.matches?({:n, 1.0}, {:n, 1})
      false

      iex> Tuplex.Template.matches?({:cfg, %{a: 1}}, {:cfg, %{a: 1, b: 2}})
      false
  """
  @spec matches?(template(), t()) :: boolean()
  def matches?(template, tuple) when is_tuple(template) and is_tuple(tuple) do
    match(template, tuple)
  end

  # -- matching ---------------------------------------------------------------

  defp match(@wildcard, _term), do: true

  defp match(template, term) when is_tuple(template) and is_tuple(term) do
    tuple_size(template) == tuple_size(term) and
      match(Tuple.to_list(template), Tuple.to_list(term))
  end

  defp match([th | tt], [h | t]), do: match(th, h) and match(tt, t)

  # Maps land here and are compared with ===, which is the =:= the guards use.
  defp match(template, term), do: template === term

  # -- compilation ------------------------------------------------------------

  defp build_terms([h | t], n, guards) do
    {pattern, n, guards} = build(h, n, guards)
    {rest, n, guards} = build_terms(t, n, guards)
    {[pattern | rest], n, guards}
  end

  defp build_terms([], n, guards), do: {[], n, guards}

  # An improper tail is a term in its own right, so compile it like any other.
  defp build_terms(tail, n, guards), do: build(tail, n, guards)

  defp build(term, n, guards) do
    cond do
      # No map anywhere: the term is already a legal head pattern, wildcards included.
      not contains_map?(term) ->
        check_reserved!(term)
        {term, n, guards}

      # Map-bearing but wildcard-free: hoist the whole thing into one equality guard.
      not contains_wildcard?(term) ->
        var = :"$#{n}"
        {var, n + 1, [{:"=:=", var, {:const, term}} | guards]}

      # Both a map and a wildcard. If this *is* the map, the wildcard is inside it.
      is_map(term) ->
        throw({:invalid, {:wildcard_in_map, term}})

      # Otherwise descend: some element holds the map, another holds the wildcard.
      is_tuple(term) ->
        {patterns, n, guards} = build_terms(Tuple.to_list(term), n, guards)
        {List.to_tuple(patterns), n, guards}

      is_list(term) ->
        build_terms(term, n, guards)
    end
  end

  defp contains_map?(term) when is_map(term), do: true

  defp contains_map?(term) when is_tuple(term),
    do: term |> Tuple.to_list() |> Enum.any?(&contains_map?/1)

  defp contains_map?([h | t]), do: contains_map?(h) or contains_map?(t)
  defp contains_map?(_term), do: false

  defp contains_wildcard?(@wildcard), do: true

  # Map.to_list rather than Enum, so structs — which are maps but not Enumerable — walk too.
  defp contains_wildcard?(term) when is_map(term) do
    term
    |> Map.to_list()
    |> Enum.any?(fn {k, v} -> contains_wildcard?(k) or contains_wildcard?(v) end)
  end

  defp contains_wildcard?(term) when is_tuple(term),
    do: term |> Tuple.to_list() |> Enum.any?(&contains_wildcard?/1)

  defp contains_wildcard?([h | t]), do: contains_wildcard?(h) or contains_wildcard?(t)
  defp contains_wildcard?(_term), do: false

  # Only ever walks terms bound for the head; hoisted subterms are exempt by construction.
  defp check_reserved!(atom) when is_atom(atom) do
    if reserved?(atom), do: throw({:invalid, {:reserved_atom, atom}}), else: :ok
  end

  defp check_reserved!(term) when is_tuple(term),
    do: term |> Tuple.to_list() |> Enum.each(&check_reserved!/1)

  defp check_reserved!([h | t]) do
    check_reserved!(h)
    check_reserved!(t)
  end

  defp check_reserved!(_term), do: :ok

  # -- validation -------------------------------------------------------------

  defp check_shape(term) when not is_tuple(term), do: {:error, :not_a_tuple}
  defp check_shape({}), do: {:error, :empty_tuple}
  defp check_shape(_tuple), do: :ok

  defp check_tag(@wildcard), do: {:error, :wildcard_tag}

  defp check_tag(tag) when is_atom(tag) do
    if reserved?(tag), do: {:error, {:reserved_atom, tag}}, else: :ok
  end

  defp check_tag(tag), do: {:error, {:non_atom_tag, tag}}

  # `:_` is the wildcard, not a match variable, and is handled before this is reached.
  defp reserved?(@wildcard), do: false

  defp reserved?(atom) do
    case Atom.to_string(atom) do
      "$" <> _ -> true
      _ -> false
    end
  end

  # -- error messages ---------------------------------------------------------

  defp explain(:not_a_tuple), do: "expected a tuple"
  defp explain(:empty_tuple), do: "expected a tuple with at least a tag element"

  defp explain(:wildcard_tag),
    do: "the tag may not be the wildcard :_ — use Tuplex.tags/0 to fan out explicitly"

  defp explain({:non_atom_tag, tag}),
    do: "the tag must be an atom, got: #{inspect(tag)}"

  defp explain({:reserved_atom, atom}),
    do: "#{inspect(atom)} is reserved — ETS reads $-prefixed atoms as match variables"

  defp explain({:wildcard_in_map, map}),
    do:
      "wildcards inside maps are not supported (ETS would match the map partially), " <>
        "got: #{inspect(map)}"
end
