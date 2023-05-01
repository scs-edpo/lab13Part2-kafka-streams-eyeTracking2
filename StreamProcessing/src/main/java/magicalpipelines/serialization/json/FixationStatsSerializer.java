package magicalpipelines.serialization.json;

import com.google.gson.Gson;
import magicalpipelines.model.aggregations.FixationStats;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

class FixationStatsSerializer implements Serializer<FixationStats> {
  private Gson gson = new Gson();

  @Override
  public byte[] serialize(String topic, FixationStats fixationStats) {
    if (fixationStats == null) return null;
    return gson.toJson(fixationStats).getBytes(StandardCharsets.UTF_8);
  }
}
