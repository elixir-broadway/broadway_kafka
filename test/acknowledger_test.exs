defmodule BroadwayKafka.AcknowledgerTest do
  use ExUnit.Case, async: true

  alias BroadwayKafka.Acknowledger, as: Ack

  @foo {1, "foo", 1}
  @bar {1, "bar", 2}
  @ack Ack.add(Ack.new(), [{1, "foo", 1, 10}, {1, "bar", 2, 0}])

  test "new" do
    assert Ack.new() == %{}
  end

  test "add" do
    assert @ack == %{
             @foo => {[], 10, []},
             @bar => {[], 0, []}
           }
  end

  test "keys" do
    assert Ack.keys(@ack) |> Enum.sort() == [@bar, @foo]
  end

  test "last_offset" do
    assert Ack.last_offset(@ack, @foo) == 10
    assert Ack.last_offset(@ack, @bar) == 0
  end

  test "update_current_offset" do
    ack = Ack.update_last_offset(@ack, @foo, 20, Enum.to_list(10..19))
    assert {true, 19, _} = Ack.update_current_offset(ack, @foo, Enum.to_list(9..19))

    ack = Ack.update_last_offset(@ack, @foo, 20, Enum.to_list(10..19))
    assert {false, 10, ack} = Ack.update_current_offset(ack, @foo, [10, 13, 14])
    assert {true, 19, _} = Ack.update_current_offset(ack, @foo, [11, 12, 15, 16, 17, 18, 19])

    ack = Ack.update_last_offset(@ack, @foo, 20, Enum.to_list(10..19))
    assert {false, nil, ack} = Ack.update_current_offset(ack, @foo, [13, 14])
    assert {false, nil, ack} = Ack.update_current_offset(ack, @foo, [11, 12, 15, 16, 17, 18, 19])
    assert {true, 19, _} = Ack.update_current_offset(ack, @foo, [10])

    ack = Ack.update_last_offset(@ack, @foo, 20, Enum.to_list(10..19))
    assert {false, nil, ack} = Ack.update_current_offset(ack, @foo, [13, 14])
    assert {false, 16, ack} = Ack.update_current_offset(ack, @foo, [10, 11, 12, 15, 16, 18, 19])
    assert {true, 19, _} = Ack.update_current_offset(ack, @foo, [17])
  end

  test "update_last_offset preserves set semantics with overlapping offsets" do
    # Publish 10..14, acknowledge 10..12.
    ack = Ack.update_last_offset(@ack, @foo, 15, Enum.to_list(10..14))
    assert {false, 12, ack} = Ack.update_current_offset(ack, @foo, [10, 11, 12])

    # Publish 13..17 (overlapping with previous), acknowledge all.
    ack = Ack.update_last_offset(ack, @foo, 18, Enum.to_list(13..17))
    assert {true, 17, _} = Ack.update_current_offset(ack, @foo, [13, 14, 15, 16, 17])
  end

  test "update_current_offset with gaps" do
    ack = Ack.update_last_offset(@ack, @foo, 20, [11, 13, 15, 17, 19])
    assert {true, 19, _} = Ack.update_current_offset(ack, @foo, [9, 11, 13, 15, 17, 19])

    ack = Ack.update_last_offset(@ack, @foo, 20, [11, 13, 15, 17, 19])
    assert {false, 12, ack} = Ack.update_current_offset(ack, @foo, [11, 15])
    assert {true, 19, _} = Ack.update_current_offset(ack, @foo, [13, 17, 19])

    ack = Ack.update_last_offset(@ack, @foo, 20, [11, 13, 15, 17, 19])
    assert {false, nil, ack} = Ack.update_current_offset(ack, @foo, [13])
    assert {false, nil, ack} = Ack.update_current_offset(ack, @foo, [15, 17, 19])
    assert {true, 19, _} = Ack.update_current_offset(ack, @foo, [11])

    ack = Ack.update_last_offset(@ack, @foo, 20, [11, 13, 15, 17, 19])
    assert {false, nil, ack} = Ack.update_current_offset(ack, @foo, [13])
    assert {false, 16, ack} = Ack.update_current_offset(ack, @foo, [11, 15, 19])
    assert {true, 19, _} = Ack.update_current_offset(ack, @foo, [17])
  end

  test "duplicate acknowledgements after draining do not block later offsets" do
    ack = Ack.update_last_offset(@ack, @foo, 11, [10])
    assert {true, 10, ack} = Ack.update_current_offset(ack, @foo, [10])
    assert {true, nil, ack} = Ack.update_current_offset(ack, @foo, [10, 10])
    assert ack[@foo] == {[], 11, []}

    ack = Ack.update_last_offset(ack, @foo, 14, [11, 12, 13])
    assert {false, nil, ack} = Ack.update_current_offset(ack, @foo, [12])
    assert {true, 13, ack} = Ack.update_current_offset(ack, @foo, [11, 13])
    assert Ack.all_drained?(ack)
  end

  test "duplicate offsets in one acknowledgement do not block out-of-order acknowledgements" do
    ack = Ack.update_last_offset(@ack, @foo, 15, [10, 11, 12, 13, 14])
    assert {false, nil, ack} = Ack.update_current_offset(ack, @foo, [11, 11, 12, 12, 13])
    assert ack[@foo] == {[10, 11, 12, 13, 14], 15, [11, 12, 13]}

    assert {false, 13, ack} = Ack.update_current_offset(ack, @foo, [10])
    assert ack[@foo] == {[14], 15, []}
    assert {true, 14, ack} = Ack.update_current_offset(ack, @foo, [14, 14])
    assert Ack.all_drained?(ack)
  end

  test "an acknowledgement can repeat an offset already in seen" do
    ack = Ack.update_last_offset(@ack, @foo, 14, [10, 11, 12, 13])
    assert {false, nil, ack} = Ack.update_current_offset(ack, @foo, [11, 12])
    assert {false, 12, ack} = Ack.update_current_offset(ack, @foo, [10, 11])
    assert ack[@foo] == {[13], 14, []}
    assert {true, 13, ack} = Ack.update_current_offset(ack, @foo, [13])
    assert Ack.all_drained?(ack)
  end

  test "stale and duplicate seen offsets do not block pending acknowledgements" do
    # Match the commit freeze: 803 is stale, and 804 appears twice in seen.
    pending = Enum.to_list(804..903)
    seen = [803, 804, 804] ++ Enum.to_list(805..899)
    ack = %{@ack | @foo => {pending, 904, seen}}

    assert {false, 900, ack} = Ack.update_current_offset(ack, @foo, [900])
    assert ack[@foo] == {[901, 902, 903], 904, []}
    assert {false, nil, ack} = Ack.update_current_offset(ack, @foo, [903])
    assert {true, 903, ack} = Ack.update_current_offset(ack, @foo, [901, 902])
    assert Ack.all_drained?(ack)
  end

  test "stale seen offsets in gaps do not block later acknowledgements" do
    ack = Ack.update_last_offset(@ack, @foo, 20, [10, 13, 19])
    assert {false, nil, ack} = Ack.update_current_offset(ack, @foo, [11, 13, 13, 19])
    assert {true, 19, ack} = Ack.update_current_offset(ack, @foo, [10])
    assert Ack.all_drained?(ack)
  end

  test "acknowledgements without pending messages do not acknowledge future messages" do
    assert {true, nil, ack} = Ack.update_current_offset(@ack, @foo, [9, 10, 11])
    ack = Ack.update_last_offset(ack, @foo, 12, [10, 11])
    assert {false, 10, ack} = Ack.update_current_offset(ack, @foo, [10])
    assert ack[@foo] == {[11], 12, []}
    assert {true, 11, _ack} = Ack.update_current_offset(ack, @foo, [11])
  end

  test "all_drained?" do
    ack = @ack
    assert Ack.all_drained?(ack)

    ack = Ack.update_last_offset(ack, @foo, 100, Enum.to_list(10..99))
    refute Ack.all_drained?(ack)

    assert {false, 49, ack} = Ack.update_current_offset(ack, @foo, Enum.to_list(10..49))
    refute Ack.all_drained?(ack)

    assert {true, 99, ack} = Ack.update_current_offset(ack, @foo, Enum.to_list(50..99))
    assert Ack.all_drained?(ack)
  end

  # Some poor man's property based testing.
  describe "property based testing" do
    test "duplicate acknowledgements never commit past an unacknowledged offset" do
      offsets = Enum.take_every(10..99, 3)
      last = List.last(offsets) + 1

      for n_parts <- 1..9 do
        ack = Ack.update_last_offset(@ack, @foo, last, offsets)
        groups = Enum.group_by(offsets ++ offsets ++ offsets, fn _ -> :rand.uniform(n_parts) end)

        {ack, _, _} =
          Enum.reduce(Map.values(groups), {ack, MapSet.new(), hd(offsets)}, fn group,
                                                                               {ack, acked, next} ->
            acked = MapSet.union(acked, MapSet.new(group))
            pending = Enum.drop_while(offsets, &MapSet.member?(acked, &1))
            new_next = List.first(pending) || last
            expected_commit = if new_next > next, do: new_next - 1, else: nil

            {drained?, commit, ack} = Ack.update_current_offset(ack, @foo, Enum.sort(group))

            assert commit == expected_commit
            assert drained? == (pending == [])
            assert ack[@foo] == {pending, last, Enum.filter(pending, &MapSet.member?(acked, &1))}

            {ack, acked, new_next}
          end)

        assert Ack.all_drained?(ack)
      end
    end

    # We generate a list from 10..99 and we break it into 1..9 random parts.
    test "drained?" do
      ack = Ack.update_last_offset(@ack, @foo, 100, Enum.to_list(10..99))

      for n_parts <- 1..9 do
        groups = Enum.group_by(10..99, fn _ -> :rand.uniform(n_parts) end)
        offsets = Map.values(groups)

        {drained?, _, ack} =
          Enum.reduce(offsets, {false, :unused, ack}, fn offset, {false, _, ack} ->
            Ack.update_current_offset(ack, @foo, Enum.sort(offset))
          end)

        assert drained?
        assert Ack.all_drained?(ack)
      end
    end
  end
end
