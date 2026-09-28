defmodule BroadwayKafka.ProducerPollingTest do
  use ExUnit.Case, async: true

  alias BroadwayKafka.{Acknowledger, Producer}

  @key {38, "message_events", 14}

  defmodule FixedOffsetClient do
    import Record, only: [defrecordp: 2, extract: 2]
    defrecordp :kafka_message, extract(:kafka_message, from_lib: "brod/include/brod.hrl")

    def fetch(_client, topic, partition, offset, _opts, config) do
      send(config.test_pid, {:fetched, topic, partition, offset})

      messages =
        for offset_in_log <- 802..806, offset_in_log >= offset do
          kafka_message(offset: offset_in_log, value: "message")
        end

      {:ok, {807, messages}}
    end
  end

  setup do
    table = :ets.new(:draining, [:set])
    :ets.insert(table, {:draining, false})

    state = %{
      acks: Acknowledger.add(Acknowledger.new(), [{38, "message_events", 14, 802}]),
      buffer: :queue.new(),
      client: FixedOffsetClient,
      client_id: :test_client,
      config: %{test_pid: self(), fetch_config: %{max_fetch_retries: 0}},
      demand: 0,
      draining_after_revoke_flag: table,
      fenced?: false,
      max_demand: 1,
      receive_interval: 1_000,
      receive_timer: nil,
      shutting_down?: false
    }

    %{state: state}
  end

  test "a round timer cannot cause buffered records to be fetched twice", %{state: state} do
    state = %{state | receive_interval: 0}
    {:noreply, [], state} = Producer.handle_demand(1, state)

    # With separate timers, the round marker can arrive before the partition
    # polls when the process moves between schedulers. Select that order here.
    # With one round timer, :poll queues both the polls and their marker in order.
    assert_receive round_timer when round_timer in [:poll, :maybe_schedule_poll]
    {:noreply, [], state} = Producer.handle_info(round_timer, state)
    {messages, state} = finish_poll_round(state)

    {:noreply, buffered, state} = Producer.handle_demand(8, state)
    assert Enum.map(messages ++ buffered, & &1.metadata.offset) == Enum.to_list(802..806)
    assert :queue.is_empty(state.buffer)
    assert_receive {:fetched, "message_events", 14, 802}
    refute_received {:fetched, "message_events", 14, 803}
  end

  test "demand between a poll and its marker drains the buffer before fetching again", %{
    state: state
  } do
    {:noreply, [], state} = Producer.handle_demand(1, state)
    assert_receive :poll
    {:noreply, [], state} = Producer.handle_info(:poll, state)
    assert_receive {:poll, @key}
    {:noreply, [first], state} = Producer.handle_info({:poll, @key}, state)

    {:noreply, buffered, state} = Producer.handle_demand(2, state)
    assert Enum.map([first | buffered], & &1.metadata.offset) == [802, 803, 804]
    {[], state} = finish_poll_round(state)

    {:noreply, buffered, state} = Producer.handle_demand(3, state)
    assert Enum.map(buffered, & &1.metadata.offset) == [805, 806]
    assert_receive :poll
    {:noreply, [], state} = Producer.handle_info(:poll, state)
    {[], state} = finish_poll_round(state)

    assert_receive {:fetched, "message_events", 14, 802}
    assert_receive {:fetched, "message_events", 14, 807}
    assert is_integer(Process.read_timer(state.receive_timer))
    Process.cancel_timer(state.receive_timer)
  end

  test "a fenced producer ignores a round timer", %{state: state} do
    state = %{state | fenced?: true}
    assert {:noreply, [], ^state} = Producer.handle_info(:poll, state)
    refute_received {:poll, _}
    refute_received :maybe_schedule_poll
  end

  defp finish_poll_round(state, messages \\ []) do
    receive do
      {:poll, _key} = poll ->
        {:noreply, emitted, state} = Producer.handle_info(poll, state)
        finish_poll_round(state, messages ++ emitted)

      :maybe_schedule_poll ->
        {:noreply, emitted, state} = Producer.handle_info(:maybe_schedule_poll, state)
        {messages ++ emitted, state}
    after
      1_000 -> flunk("poll round did not finish")
    end
  end
end
