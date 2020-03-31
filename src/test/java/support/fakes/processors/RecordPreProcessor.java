package support.fakes.processors;

import java.util.concurrent.CompletableFuture;

import io.vertx.core.json.JsonObject;

@FunctionalInterface
public interface RecordPreProcessor {

  CompletableFuture<JsonObject> process(JsonObject oldItem, JsonObject newItem) throws Exception;
}
