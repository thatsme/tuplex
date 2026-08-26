defmodule Tuplex.Watch do
  @moduledoc false

  # NOTE: internal. The prose below is kept as a design record but is not published.
  #
  #   One supervised process per subscription, holding a watch registration alive across shard
  #   restarts.
  #
  #   A watcher is not sitting in a call the way a blocked `in/2` caller is, so it has no way to
  #   notice that its shard died and nothing to re-register from. This process is that
  #   somewhere: it registers the subscription with the shard, monitors the shard so it can
  #   register again with the replacement, and monitors the subscriber so the registration goes
  #   away when its audience does.
  #
  #   Events are sent to the subscriber **directly by the shard**, not relayed through here.
  #   This process is only the registration's keeper, so a slow subscriber cannot back up behind
  #   it and the message path stays one hop.

  use GenServer, restart: :temporary

  alias Tuplex.Shard

  @registry Tuplex.WatchRegistry
  @supervisor Tuplex.WatchSupervisor

  @doc false
  def start(spec) do
    DynamicSupervisor.start_child(@supervisor, {__MODULE__, spec})
  end

  @doc false
  def start_link(spec) do
    GenServer.start_link(__MODULE__, spec, name: {:via, Registry, {@registry, spec.ref}})
  end

  @doc """
  Ends the subscription identified by `ref`.
  """
  @spec stop(reference()) :: :ok
  def stop(ref) do
    case Registry.lookup(@registry, ref) do
      [{pid, _value}] -> GenServer.stop(pid, :normal)
      [] -> :ok
    end
  catch
    # Already gone, which is the state the caller wanted.
    :exit, _reason -> :ok
  end

  @impl true
  def init(spec) do
    Process.flag(:trap_exit, true)
    subscriber = Process.monitor(spec.subscriber)

    case attach(spec) do
      {:ok, shard} -> {:ok, Map.merge(spec, %{shard: shard, subscriber_ref: subscriber})}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{subscriber_ref: ref} = state) do
    # Nobody left to tell. The shard drops the registration when this process exits.
    {:stop, :normal, state}
  end

  def handle_info({:DOWN, ref, :process, _dead, _reason}, %{shard: {_shard_pid, ref}} = state) do
    # The shard died holding the registration. A watch is a standing interest in a tag, and
    # a crash does not withdraw it, so register again with the replacement.
    case attach(state) do
      {:ok, shard} -> {:noreply, %{state | shard: shard}}
      {:error, reason} -> {:stop, reason, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, state) do
    Shard.unregister_watch(state.tag, state.ref)
    :ok
  catch
    # The shard is gone too, so there is nothing left to unregister from.
    :exit, _reason -> :ok
  end

  defp attach(spec) do
    with :ok <-
           Shard.register_watch(
             spec.tag,
             spec.ref,
             spec.template,
             spec.events,
             spec.subscriber,
             spec.max_queue
           ),
         {:ok, pid, _tab} <- Shard.lookup(spec.tag) do
      {:ok, {pid, Process.monitor(pid)}}
    else
      {:error, reason} -> {:error, reason}
      :error -> {:error, :no_shard}
    end
  end
end
