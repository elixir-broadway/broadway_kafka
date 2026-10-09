defmodule BroadwayKafka.NonSharedClientIntegrationTest do
  use ExUnit.Case

  @moduletag :integration

  defmodule Pipeline do
    use Broadway

    def start_link(opts) do
      Broadway.start_link(__MODULE__,
        name: opts[:name],
        context: %{test_pid: opts[:test_pid]},
        producer: [
          module:
            {BroadwayKafka.Producer,
             [
               hosts: BroadwayKafka.NonSharedClientIntegrationTest.hosts(),
               group_id: opts[:group_id],
               topics: [opts[:topic]],
               receive_interval: 50,
               offset_reset_policy: :earliest
             ]},
          concurrency: 1
        ],
        processors: [default: [concurrency: 1]]
      )
    end

    @impl true
    def handle_message(_processor, message, context) do
      send(context.test_pid, {:message, message.data})
      message
    end
  end

  def hosts do
    port =
      "BROADWAY_KAFKA_TEST_PORT"
      |> System.get_env("9092")
      |> String.to_integer()

    [localhost: port]
  end

  test "a pipeline restarts after its non-shared client went down" do
    Process.flag(:trap_exit, true)

    suffix = System.os_time()
    topic = "broadway-kafka-non-shared-client-#{suffix}"
    group_id = "broadway-kafka-non-shared-client-group-#{suffix}"
    client_id = :"broadway_kafka_non_shared_client_#{suffix}"
    name = :"BroadwayKafkaNonSharedClient#{suffix}"
    opts = [name: name, test_pid: self(), topic: topic, group_id: group_id]

    create_topic(topic)

    :ok = :brod.start_client(hosts(), client_id)
    :ok = :brod.start_producer(client_id, topic, [])

    on_exit(fn -> :brod.stop_client(client_id) end)

    {:ok, pipeline} = Pipeline.start_link(opts)

    produce(client_id, topic, "before client went down")
    assert_receive {:message, "before client went down"}, 10_000

    [producer_name] = Broadway.producer_names(name)
    client_pid = Process.whereis(Module.concat([producer_name, Client]))
    ref = Process.monitor(client_pid)

    # :brod_sup restarts the client after a delay, keeping it registered meanwhile
    Process.exit(client_pid, :kill)
    assert_receive {:DOWN, ^ref, _, _, _}

    stop_broadway(pipeline)

    assert {:ok, pipeline} = Pipeline.start_link(opts)
    on_exit(fn -> stop_broadway(pipeline) end)

    produce(client_id, topic, "after restart")
    assert_receive {:message, "after restart"}, 10_000
  end

  defp create_topic(topic) do
    topic_config = [
      %{
        num_partitions: 1,
        replication_factor: 1,
        name: topic,
        assignments: [],
        configs: []
      }
    ]

    wait_until_create_topic(topic_config)
  end

  defp wait_until_create_topic(topic_config) do
    case :brod.create_topics(hosts(), topic_config, %{timeout: 1_000}) do
      :ok ->
        :ok

      _error ->
        Process.sleep(10)
        wait_until_create_topic(topic_config)
    end
  end

  defp produce(client_id, topic, message) do
    :ok = :brod.produce_sync(client_id, topic, 0, "", message)
  end

  defp stop_broadway(pid) do
    if Process.alive?(pid) do
      ref = Process.monitor(pid)
      Process.exit(pid, :normal)

      receive do
        {:DOWN, ^ref, _, _, _} -> :ok
      end
    end
  end
end
