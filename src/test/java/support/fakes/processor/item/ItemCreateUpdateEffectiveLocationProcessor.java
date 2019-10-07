package support.fakes.processor.item;

import org.apache.commons.lang3.ObjectUtils;

import io.vertx.core.json.JsonObject;
import support.fakes.processor.RecordCreateProcessor;
import support.fakes.processor.RecordUpdateProcessor;


public class ItemCreateUpdateEffectiveLocationProcessor implements RecordCreateProcessor, RecordUpdateProcessor {
  @Override
  public void onCreate(JsonObject newEntity) {
    onUpdate(null, newEntity);
  }

  @Override
  public void onUpdate(JsonObject oldEntity, JsonObject newEntity) {
    String permanentLocationId = newEntity.getString("permanentLocationId");
    String temporaryLocationId = newEntity.getString("temporaryLocationId");

    if (permanentLocationId != null || temporaryLocationId != null) {
      newEntity.put("effectiveLocationId",
        ObjectUtils.firstNonNull(temporaryLocationId, permanentLocationId));
    }
  }
}
