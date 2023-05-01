package magicalpipelines.serialization.json;

import magicalpipelines.serialization.Click;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class ClickSerdes implements Serde<Click> {

  @Override
  public Serializer<Click> serializer() {
    return new ClickSerializer();
  }

  @Override
  public Deserializer<Click> deserializer() {
    return new ClickDeserializer();
  }
}
