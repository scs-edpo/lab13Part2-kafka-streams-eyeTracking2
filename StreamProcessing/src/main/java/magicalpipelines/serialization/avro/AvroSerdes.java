package magicalpipelines.serialization.avro;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import magicalpipelines.model.TranslatedFixation;
import org.apache.kafka.common.serialization.Serde;

import java.util.Collections;
import java.util.Map;

public class AvroSerdes {

  public static Serde<TranslatedFixation> avroFixation(String url, boolean isKey) {
    Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", url);
    Serde<TranslatedFixation> serde = new SpecificAvroSerde<>();
    serde.configure(serdeConfig, isKey);
    return serde;
  }
}
