defmodule Tuplex.Template do
  @moduledoc """
  Validation, shard-key extraction, and match-spec construction for tuples and templates.

  This module is pure. It performs no side effects, owns no state, and never touches
  `:ets` — it only decides what a template *means*. `Tuplex.Store` turns those decisions
  into table operations.

  ## Tuples and templates

  A **tuple** is what you write with `Tuplex.out/2`. A **template** is what you match with
  (`in`, `rd`, `inp`, `rdp`, `rd_all`, `watch`). Both are Erlang tuples whose first element
  is an atom **tag**:

      {:job, 1, "payload"}      # a tuple
      {:job, :_, :_}            # a template matching any 3-arity :job tuple

  The tag and the arity together form the storage key, so a template only ever compares
  against tuples of the same tag *and* the same size. An arity-3 template can never match
  an arity-2 tuple.

  ## Matching is exact

  Matching uses strict equality, which is the correct Linda semantics: the float `1.0` does
  not match the stored integer `1`. Only `:_` is a wildcard, and it matches at any depth:

      matches?({:point, :_, 2}, {:point, 1, 2})           #=> true
      matches?({:point, {:x, :_}}, {:point, {:x, 9}})     #=> true
      matches?({:n, 1.0}, {:n, 1})                        #=> false

  ## What a template may not contain

  Three restrictions, all of them about avoiding silently wrong matches rather than about
  taste:

    * **The tag must be a concrete atom.** `{:_, :_}` is rejected. The tag is what selects
      the shard, so a wildcard tag would mean fanning every read out across every shard —
      out of scope for v0.1.

    * **No `:"$1"`-style atoms.** ETS reads any atom beginning with `$` as a match
      variable, so `{:x, :"$1"}` would quietly behave as a second wildcard instead of
      matching the literal atom `:"$1"`.

    * **No maps.** ETS match specs give maps *partial* matching — a `%{a: 1}` pattern
      matches a stored `%{a: 1, b: 2}`. That contradicts the exactness above, so rather
      than ship two different notions of "match", v0.1 rejects maps inside templates. Use
      `:_` for that position and filter the results yourself.

  None of these apply to tuples passed to `out/2`. Stored tuples are data: they are never
  interpreted as patterns, so they may contain `:_`, `:"$1"`, and maps freely. Only the tag
  rule is shared, because storage still has to route them.
  """

  @wildcard :_

  @typedoc "A tuple written into the space with `Tuplex.out/2`."
  @type t :: tuple()

  @typedoc "A pattern matched against stored tuples. `:_` is the wildcard."
  @type template :: tuple()

  @typedoc "The ETS storage key: the tuple's tag paired with its arity."
  @type key :: {atom(), non_neg_integer()}

  @typedoc "Why a tuple or template was rejected."
  @type error ::
          :not_a_tuple
          | :empty_tuple
          | :wildcard_tag
          | {:non_atom_tag, term()}
          | {:reserved_atom, atom()}
          | {:map_in_template, map()}

  @doc """
  Validates a template.

  Returns `{:ok, template}` unchanged, or `{:error, reason}`. See the moduledoc for what
  a template may not contain.

  ## Examples

      iex> Tuplex.Template.validate({:job, :_, 2})
      {:ok, {:job, :_, 2}}

      iex> Tuplex.Template.validate({:_, 1})
      {:error, :wildcard_tag}

      iex> Tuplex.Template.validate({:job, :"$1"})
      {:error, {:reserved_atom, :"$1"}}

      iex> Tuplex.Template.validate({:job, %{a: 1}})
      {:error, {:map_in_template, %{a: 1}}}

      iex> Tuplex.Template.validate([:job])
      {:error, :not_a_tuple}
  """
  @spec validate(term()) :: {:ok, template()} | {:error, error()}
  def validate(template) do
    with :ok <- check_shape(template),
         :ok <- check_tag(elem(template, 0)),
         :ok <- scan(Tuple.to_list(template)) do
      {:ok, template}
    end
  end

  @doc """
  Same as `validate/1` but returns the template or raises `ArgumentError`.
  """
  @spec validate!(term()) :: template()
  def validate!(template) do
    case validate(template) do
      {:ok, template} -> template
      {:error, reason} -> raise ArgumentError, "invalid template: " <> explain(reason)
    end
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
  Returns the storage key for a tuple or template: its tag paired with its arity.

  Both sides of a match agree on this key, which is what makes arity part of the match
  rather than something checked afterwards.

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
  Builds the ETS match spec that selects stored records matching `template`.

  The spec is built by hand: `:ets.fun2ms/1` is a compile-time macro and cannot see a
  template that only exists at runtime.

  The record layout it matches is `{{tag, arity}, uniq, tuple}`, where `uniq` is the
  monotonic integer that keeps otherwise-identical records distinct. The template is
  dropped straight into the third position as a nested head pattern — `:_` is already
  ETS's own wildcard, so no translation is needed — and the body `[:"$_"]` returns the
  whole record so the caller can hand it back to `:ets.delete_object/2`.

  Expects an already-validated template; see `validate/1`.

  ## Examples

      iex> Tuplex.Template.match_spec({:job, :_, 2})
      [{{{:job, 3}, :_, {:job, :_, 2}}, [], [:"$_"]}]
  """
  @spec match_spec(template()) :: [{tuple(), list(), list()}]
  def match_spec(template) do
    [{{key(template), @wildcard, template}, [], [:"$_"]}]
  end

  @doc """
  Returns whether `tuple` matches `template`, without consulting ETS.

  This is the same relation the match spec from `match_spec/1` expresses, implemented in
  plain Elixir so a process can test a freshly written tuple against waiting templates
  without a table round-trip.

  ## Examples

      iex> Tuplex.Template.matches?({:job, :_, 2}, {:job, 1, 2})
      true

      iex> Tuplex.Template.matches?({:job, :_}, {:job, 1, 2})
      false

      iex> Tuplex.Template.matches?({:n, 1.0}, {:n, 1})
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

  defp match(template, term), do: template === term

  # -- validation -------------------------------------------------------------

  defp check_shape(term) when not is_tuple(term), do: {:error, :not_a_tuple}
  defp check_shape({}), do: {:error, :empty_tuple}
  defp check_shape(_tuple), do: :ok

  defp check_tag(@wildcard), do: {:error, :wildcard_tag}

  defp check_tag(tag) when is_atom(tag) do
    if reserved?(tag), do: {:error, {:reserved_atom, tag}}, else: :ok
  end

  defp check_tag(tag), do: {:error, {:non_atom_tag, tag}}

  # Walks a template looking for terms ETS would read as something other than a literal.
  defp scan([]), do: :ok

  defp scan([term | rest]) do
    case scan_term(term) do
      :ok -> scan(rest)
      error -> error
    end
  end

  defp scan_term(map) when is_map(map), do: {:error, {:map_in_template, map}}

  defp scan_term(atom) when is_atom(atom) do
    if reserved?(atom), do: {:error, {:reserved_atom, atom}}, else: :ok
  end

  defp scan_term(tuple) when is_tuple(tuple), do: scan(Tuple.to_list(tuple))
  defp scan_term(list) when is_list(list), do: scan(flatten_tail(list, []))
  defp scan_term(_term), do: :ok

  # An improper tail is still a term worth scanning, so fold it in as a final element.
  defp flatten_tail([h | t], acc), do: flatten_tail(t, [h | acc])
  defp flatten_tail([], acc), do: :lists.reverse(acc)
  defp flatten_tail(tail, acc), do: :lists.reverse([tail | acc])

  # `:_` is the wildcard, not a reserved variable, and is handled before this is reached.
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
  defp explain(:wildcard_tag), do: "the tag may not be the wildcard :_"

  defp explain({:non_atom_tag, tag}),
    do: "the tag must be an atom, got: #{inspect(tag)}"

  defp explain({:reserved_atom, atom}),
    do: "#{inspect(atom)} is reserved — ETS reads $-prefixed atoms as match variables"

  defp explain({:map_in_template, map}),
    do: "maps are not supported in templates (ETS matches them partially), got: #{inspect(map)}"
end
