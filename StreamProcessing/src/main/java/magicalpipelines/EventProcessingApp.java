package magicalpipelines;

import magicalpipelines.topology.EyeTrackingTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.HostInfo;

import java.util.Properties;

class EventProcessingApp {
  public static void main(String[] args) {
    Topology topology = EyeTrackingTopology.build();

    // set the required properties for running Kafka Streams
    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "dev");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
    config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

    // config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    // config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    // config.put("schema.registry.url", "http://localhost:8081");

    // build the topology and start streaming!
    KafkaStreams streams = new KafkaStreams(topology, config);

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    System.out.println("Starting app");
    streams.start();

    // start the REST service
    // start the REST service
    HostInfo hostInfo = new HostInfo("localhost", 7070);
    MonitorService service = new MonitorService(hostInfo, streams);
    service.start();

  }
}
