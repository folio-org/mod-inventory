package org.folio.inventory.dataimport.handlers.actions;

import static org.apache.commons.lang3.StringUtils.isEmpty;

import java.util.Map;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.folio.inventory.common.Context;
import org.folio.inventory.domain.instances.Instance;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.Record;

public class UpdateInstanceEventHandler {

  private static final Logger LOGGER = LogManager.getLogger(UpdateInstanceEventHandler.class);

  private static final String MARC_KEY = "MARC";
  private static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";

  private final Context context;
  private final InstanceUpdateDelegate instanceUpdateDelegate;

  public UpdateInstanceEventHandler(InstanceUpdateDelegate updateInstanceDelegate, Context context) {
    this.context = context;
    this.instanceUpdateDelegate = updateInstanceDelegate;
  }

  public Future<Instance> handle(Map<String, String> eventPayload) {
    Promise<Instance> future = Promise.promise();
    try {
      if (isValidPayload(eventPayload)) {
        String message = "Event does not contain required data to update Instance";
        LOGGER.error(message);
        future.fail(new EventProcessingException(message));
        return future.future();
      }

      Record marcRecord = new JsonObject(eventPayload.get(MARC_KEY)).mapTo(Record.class);
      instanceUpdateDelegate.handle(eventPayload, marcRecord, context)
        .onComplete(ar -> {
          if (ar.succeeded()) {
            future.complete(ar.result());
          } else {
            future.fail(ar.cause());
          }
        });
    } catch (Exception e) {
      LOGGER.error("Failed to update Instance", e);
      future.fail(e);
    }
    return future.future();
  }

  private boolean isValidPayload(Map<String, String> eventPayload) {
    return eventPayload == null || isEmpty(eventPayload.get(MARC_KEY)) || isEmpty(eventPayload.get(MAPPING_RULES_KEY))
      || isEmpty(eventPayload.get(MAPPING_PARAMS_KEY));
  }
}
