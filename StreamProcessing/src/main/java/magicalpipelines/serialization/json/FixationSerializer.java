package magicalpipelines.serialization.json;

import com.google.gson.Gson;
import magicalpipelines.serialization.Fixation;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

class FixationSerializer implements Serializer<Fixation> {
  private Gson gson = new Gson();

  @Override
  public byte[] serialize(String topic, Fixation fixation) {
    if (fixation == null) return null;
    return gson.toJson(fixation).getBytes(StandardCharsets.UTF_8);
  }
}
