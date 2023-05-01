package magicalpipelines.serialization.json;

import com.google.gson.Gson;
import magicalpipelines.model.join.FixationClick;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

class FixationClickSerializer implements Serializer<FixationClick> {
  private Gson gson = new Gson();

  @Override
  public byte[] serialize(String topic, FixationClick fixationClick) {
    if (fixationClick == null) return null;
    return gson.toJson(fixationClick).getBytes(StandardCharsets.UTF_8);
  }
}
