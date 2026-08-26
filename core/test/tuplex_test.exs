defmodule TuplexTest do
  use ExUnit.Case
  doctest Tuplex

  test "greets the world" do
    assert Tuplex.hello() == :world
  end
end
