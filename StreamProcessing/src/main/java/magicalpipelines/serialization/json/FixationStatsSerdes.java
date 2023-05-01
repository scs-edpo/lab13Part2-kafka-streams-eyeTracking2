package magicalpipelines.serialization.json;

import magicalpipelines.model.aggregations.FixationStats;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class FixationStatsSerdes implements Serde<FixationStats> {

  @Override
  public Serializer<FixationStats> serializer() {
    return new FixationStatsSerializer();
  }

  @Override
  public Deserializer<FixationStats> deserializer() {
    return new FixationStatsDeserializer();
  }
}
