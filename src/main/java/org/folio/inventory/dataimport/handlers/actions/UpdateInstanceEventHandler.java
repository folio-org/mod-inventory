package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.inventory.common.Context;
import org.folio.inventory.domain.instances.Instance;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.util.OkapiConnectionParams;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.inventory.dataimport.util.EventHandlingUtil.sendEventWithPayload;

public class UpdateInstanceEventHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(UpdateInstanceEventHandler.class);
  private static final String MARC_KEY = "MARC";
  private static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";
  private static final String DI_INDICATOR = "DI";

  private final Context context;
  private final InstanceUpdateDelegate instanceUpdateDelegate;

  public UpdateInstanceEventHandler(InstanceUpdateDelegate updateInstanceDelegate, Context context) {
    this.context = context;
    this.instanceUpdateDelegate = updateInstanceDelegate;
  }

  public CompletableFuture<Instance> handle(Map<String, String> eventPayload, Map<String, String> requestHeaders, Vertx vertx) {
    CompletableFuture<Instance> future = new CompletableFuture<>();
    try {
      if (eventPayload == null || isEmpty(eventPayload.get(MARC_KEY)) || isEmpty(eventPayload.get(MAPPING_RULES_KEY)) || isEmpty(eventPayload.get(MAPPING_PARAMS_KEY))) {
        String message = "Event does not contain required data to update Instance";
        LOGGER.error(message);
        future.completeExceptionally(new EventProcessingException(message));
        return future;
      }

      Record marcRecord = new JsonObject(eventPayload.get(MARC_KEY)).mapTo(Record.class);
      instanceUpdateDelegate.handle(eventPayload, marcRecord, context)
        .onComplete(ar -> {
          if (isEmpty(eventPayload.get((DI_INDICATOR)))) {
            OkapiConnectionParams params = new OkapiConnectionParams(requestHeaders, vertx);
            if (ar.succeeded()) {
              sendEventWithPayload(Json.encode(eventPayload), "QM_INVENTORY_INSTANCE_UPDATED", params)
                .onComplete(v -> future.complete(ar.result()));
            } else {
              sendEventWithPayload(Json.encode(eventPayload), "QM_ERROR", params)
                .onComplete(v -> future.completeExceptionally(ar.cause()));
            }
          }
        });
    } catch (Exception e) {
      LOGGER.error("Failed to update Instance", e);
      future.completeExceptionally(e);
    }
    return future;
  }
}
