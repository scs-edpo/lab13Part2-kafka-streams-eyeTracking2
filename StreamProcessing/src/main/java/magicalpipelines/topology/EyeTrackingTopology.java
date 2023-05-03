package magicalpipelines.topology;

import magicalpipelines.model.TranslatedFixation;
import magicalpipelines.model.aggregations.FixationStats;
import magicalpipelines.model.join.FixationClick;
import magicalpipelines.partitioner.CustomPartitionerTranslatedFixation;
import magicalpipelines.serialization.Click;
import magicalpipelines.serialization.Fixation;
import magicalpipelines.serialization.avro.AvroSerdes;
import magicalpipelines.serialization.json.ClickSerdes;
import magicalpipelines.serialization.json.FixationStatsSerdes;
import magicalpipelines.serialization.json.FixationClickSerdes;
import magicalpipelines.serialization.json.FixationSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import static magicalpipelines.fixationProcessing.Processor.findAOI;

public class EyeTrackingTopology {


    public static Topology build() {

        // the builder is used to construct the topology
        StreamsBuilder builder = new StreamsBuilder();

        // start streaming fixations using our custom value serdes.
        KStream<byte[], Fixation> fixationStream =
                builder.stream("fixations", Consumed.with(Serdes.ByteArray(), new FixationSerdes()));
        fixationStream.print(Printed.<byte[], Fixation>toSysOut().withLabel("fixations-stream"));

        // start streaming clicks using our custom value serdes.
        KStream<byte[], Click> clickStream =
                builder.stream("clicks", Consumed.with(Serdes.ByteArray(), new ClickSerdes()));
        clickStream.print(Printed.<byte[], Click>toSysOut().withLabel("clicks-stream"));


        // Stateless processing

        // Apply content filter to fixations // Keep only relevant attributes
        KStream<byte[], Fixation> contentFilteredFixations =
                fixationStream.mapValues(
                        (fixation) -> {
                            Fixation contentFilteredFixation = new Fixation();
                            contentFilteredFixation.setTimestamp(fixation.getTimestamp());
                            contentFilteredFixation.setXpos(fixation.getXpos());
                            contentFilteredFixation.setYpos(fixation.getYpos());
                            contentFilteredFixation.setFixationDuration(fixation.getFixationDuration());
                            contentFilteredFixation.setPupilSize(fixation.getPupilSize());
                            return contentFilteredFixation;
                        });


        // Apply event filter to fixations
        // Keep only fixations with duration above or equal to 60 ms
        KStream<byte[], Fixation> eventFilteredFixations =
                contentFilteredFixations.filter(
                        (key, fixation) -> {
                            return (fixation.getFixationDuration()>=60);
                        });

        // Apply event translator
        // find AOI based on xpos and ypos
        KStream<byte[], TranslatedFixation> eventTranslatedFixations =
                eventFilteredFixations.mapValues(
                        (fixation) -> {

                            String aoi = findAOI(fixation.getXpos(),fixation.getYpos()).toString();

                            TranslatedFixation translatedFixation =
                                    TranslatedFixation.newBuilder()
                                            .setTimestamp(fixation.getTimestamp())
                                            .setXpos(fixation.getXpos())
                                            .setYpos(fixation.getYpos())
                                            .setPupilSize(fixation.getPupilSize())
                                            .setFixationDuration(fixation.getFixationDuration())
                                            .setAOI(aoi)
                                            .build();

                            return translatedFixation;
                        });



        // Apply event router
        // For sake of simulation divide fixations into two fixationBranches based on fixationDurationThreshold
        // fixation with duration less than FixationDurationThreshold are assumed to reflect low cognitive load, while the other fixations are assumed to reflect higher cognitive load
        double fixationDurationThreshold = 250;
        KStream<byte[], TranslatedFixation>[] fixationBranches = eventTranslatedFixations.branch(
                (k, fixation) -> fixation.getFixationDuration() < fixationDurationThreshold,
                (k, fixation) -> fixation.getFixationDuration() >= fixationDurationThreshold);

        // Route fixationBranches to different partitions of the same topic and process them
        for (int i = 0; i < fixationBranches.length; i++) {

            // Select branch
            KStream<byte[], TranslatedFixation> branch = fixationBranches[i];

            // Create a new key based on the partitioning condition
            int fi = i;
            KStream<String, TranslatedFixation> keyedStream = branch.selectKey((key, value) -> {
                if (fi==0) {
                    return "low CL";
                } else {
                    return "high CL";
                }
            });

            // Write to the output topic
            keyedStream.to(
                    "fixations-out",
                    Produced.with(
                            Serdes.String(),
                            AvroSerdes.avroFixation("http://localhost:8081", false),
                            new CustomPartitionerTranslatedFixation()));
        }

        // Statefull processing


        // group fixations by AOI
        KGroupedStream<String, TranslatedFixation> groupedFixationsByAOI =
                eventTranslatedFixations.groupBy(
                        (key, value) -> value.getAOI().toString(),
                        Grouped.<String, TranslatedFixation>with(Serdes.String(), AvroSerdes.avroFixation("http://localhost:8081", false))
                );

        // group clicks by AOI
        KGroupedStream<String, Click> groupedClicksByAOI =
                clickStream.groupBy(
                        (key, value) -> value.getAoi(),
                        Grouped.<String, Click>with(Serdes.String(), new ClickSerdes()));


        // aggregation: fixation count, average fixation duration, total fixation duration per AOI
        Initializer<FixationStats> fixationStatsInitializer = () -> new FixationStats(0,0,0);

        Aggregator<String, TranslatedFixation, FixationStats> fixationStatsAggregator = (key, fixation, fixationStats) -> {
            long newFixationCount = fixationStats.getFixationCount() + 1;
            double newTotalFixationDuration = fixationStats.getTotalFixationDuration() + fixation.getFixationDuration();
            double newAverageFixationDuration = newTotalFixationDuration / newFixationCount;
            return new FixationStats(newFixationCount, newTotalFixationDuration, newAverageFixationDuration);
        };

        KTable<String, FixationStats> fixationStatsByAOI = groupedFixationsByAOI.aggregate(
                fixationStatsInitializer,
                fixationStatsAggregator,
                Materialized.<String, FixationStats, KeyValueStore<Bytes, byte[]>>as("fixationStats")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(new FixationStatsSerdes())
        );


        // aggregation: click count by AOI
        KTable<String, Long> clickCountByAOI = groupedClicksByAOI.count(Materialized.as("clickCount"));


        // Perform a join on fixationStatsByAOI and clickCountByAOI KTables
        KTable<String, FixationClick> fixationAndClickCountsByAOI = fixationStatsByAOI.join(
                clickCountByAOI,
                (fixationStats, clickCount) -> new FixationClick(fixationStats, clickCount),
                Materialized.<String, FixationClick, KeyValueStore<Bytes, byte[]>>as("FixationClickStats")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(new FixationClickSerdes())
        );


        return builder.build();
    }
}
