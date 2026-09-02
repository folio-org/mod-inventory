package support.fakes.processors;

import io.vertx.core.json.JsonObject;
import java.util.concurrent.CompletableFuture;

@FunctionalInterface
public interface RecordPreProcessor {

  CompletableFuture<JsonObject> process(String tenant, JsonObject oldItem, JsonObject newItem) throws Exception;
}
