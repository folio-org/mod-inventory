package support.fakes.processor;

import io.vertx.core.json.JsonObject;

/**
 * FakeStorageModule record update pre-processor.
 */
public interface RecordUpdateProcessor {
  /**
   * Update pre-processor.
   *
   * @param oldEntity - Old entity, nullable when no entity associated with the ID.
   * @param newEntity - New entity
   */
  void onUpdate(JsonObject oldEntity, JsonObject newEntity);
}
