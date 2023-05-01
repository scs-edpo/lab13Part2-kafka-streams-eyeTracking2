package magicalpipelines.serialization.json;

import magicalpipelines.model.join.FixationClick;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class FixationClickSerdes implements Serde<FixationClick> {

  @Override
  public Serializer<FixationClick> serializer() {
    return new FixationClickSerializer();
  }

  @Override
  public Deserializer<FixationClick> deserializer() {
    return new FixationClickDeserializer();
  }
}
