package magicalpipelines.serialization.json;

import magicalpipelines.serialization.Fixation;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class FixationSerdes implements Serde<Fixation> {

  @Override
  public Serializer<Fixation> serializer() {
    return new FixationSerializer();
  }

  @Override
  public Deserializer<Fixation> deserializer() {
    return new FixationDeserializer();
  }
}
