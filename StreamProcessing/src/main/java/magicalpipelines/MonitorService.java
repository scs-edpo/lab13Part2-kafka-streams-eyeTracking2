package magicalpipelines;

import io.javalin.Javalin;
import io.javalin.http.Context;
import java.util.HashMap;
import java.util.Map;

import magicalpipelines.model.aggregations.FixationStats;
import magicalpipelines.model.join.FixationClick;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MonitorService {

    private final HostInfo hostInfo;
    private final KafkaStreams streams;

    // Logger
    private static final Logger log = LoggerFactory.getLogger(MonitorService.class);

    MonitorService(HostInfo hostInfo, KafkaStreams streams) {
        this.hostInfo = hostInfo;
        this.streams = streams;
    }




    // Start the Javalin web server and configure routes
    void start() {

        // Create and start a Javalin server instance with the specified port
        Javalin app = Javalin.create(config -> {config.staticFiles.add("/public");}).start(hostInfo.port());

        // Define a route for querying in the key-value store
        app.get("/fixationMonitor", this::getFixationStats);
        app.get("/clickMonitor", this::getClickCount);
        app.get("/fixationClickMonitor", this::getFixationClickCount);

    }


    void getFixationStats(Context ctx) {
        Map<String, FixationStats> monitor = new HashMap<>();

        ReadOnlyKeyValueStore<String, FixationStats> store = streams.store(
                StoreQueryParameters.fromNameAndType(
                        "fixationStats",
                        QueryableStoreTypes.keyValueStore()));

        KeyValueIterator<String, FixationStats> range = store.all();
        while (range.hasNext()) {
            KeyValue<String, FixationStats> next = range.next();
            String aoi = next.key;
            FixationStats fixationStats = next.value;
            monitor.put(aoi, fixationStats);
        }
        range.close();
        ctx.json(monitor);
    }


    void getClickCount(Context ctx) {
        Map<String, Long> monitor = new HashMap<>();

        ReadOnlyKeyValueStore<String, Long> store = streams.store(
                StoreQueryParameters.fromNameAndType(
                        "clickCount",
                        QueryableStoreTypes.keyValueStore()));

        KeyValueIterator<String, Long> range = store.all();
        while (range.hasNext()) {
            KeyValue<String, Long> next = range.next();
            String aoi = next.key;
            long count = next.value;
            monitor.put(aoi, count);
        }
        range.close();
        ctx.json(monitor);
    }


    void getFixationClickCount(Context ctx) {
        Map<String, FixationClick> monitor = new HashMap<>();

        ReadOnlyKeyValueStore<String, FixationClick> store = streams.store(
                StoreQueryParameters.fromNameAndType(
                        "FixationClickStats",
                        QueryableStoreTypes.keyValueStore()));

        KeyValueIterator<String, FixationClick> range = store.all();
        while (range.hasNext()) {
            KeyValue<String, FixationClick> next = range.next();
            String aoi = next.key;
            monitor.put(aoi, next.value);
        }
        range.close();
        ctx.json(monitor);
    }

}
