package magicalpipelines.serialization.json;

import com.google.gson.Gson;
import magicalpipelines.serialization.Click;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

class ClickSerializer implements Serializer<Click> {
  private Gson gson = new Gson();

  @Override
  public byte[] serialize(String topic, Click click) {
    if (click == null) return null;
    return gson.toJson(click).getBytes(StandardCharsets.UTF_8);
  }
}
