package magicalpipelines.partitioner;

import magicalpipelines.model.TranslatedFixation;
import org.apache.kafka.streams.processor.StreamPartitioner;

public class CustomPartitionerTranslatedFixation implements StreamPartitioner<String, TranslatedFixation> {

    @Override
    public Integer partition(String topic, String key, TranslatedFixation value, int numPartitions) {
        if (key.equals("high CL")) {
            return 1; // Send all records with the "high CL" key to partition 1
        } else {
            return 0; // Send all records with the "low CL" key to partition 0
        }
    }
}