package support.fakes.processor;

import io.vertx.core.json.JsonObject;

/**
 * FakeStorageModule create record pre-processor.
 */
public interface RecordCreateProcessor {
  void onCreate(JsonObject newEntity);
}
