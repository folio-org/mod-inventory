package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.Context;
import org.folio.inventory.domain.instances.Instance;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.Record;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isEmpty;

public class UpdateInstanceEventHandler {

  private static final Logger LOGGER = LogManager.getLogger(UpdateInstanceEventHandler.class);

  private static final String MARC_KEY = "MARC_BIB";
  private static final String USER_CONTEXT_KEY = "USER_CONTEXT";
  private static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";

  private final Context context;
  private final InstanceUpdateDelegate instanceUpdateDelegate;
  private final PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper;

  public UpdateInstanceEventHandler(InstanceUpdateDelegate updateInstanceDelegate, Context context,
                                    PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper) {
    this.context = context;
    this.instanceUpdateDelegate = updateInstanceDelegate;
    this.precedingSucceedingTitlesHelper = precedingSucceedingTitlesHelper;
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
      Future<Instance> instanceUpdateFuture = instanceUpdateDelegate.handle(eventPayload, marcRecord, context);

      instanceUpdateFuture
        .compose(updatedInstance -> precedingSucceedingTitlesHelper.getExistingPrecedingSucceedingTitles(updatedInstance, context))
        .map(UpdateInstanceEventHandler::retrieveTitlesIds)
        .compose(titlesIds -> precedingSucceedingTitlesHelper.deletePrecedingSucceedingTitles(titlesIds, context))
        .compose(ar -> precedingSucceedingTitlesHelper.createPrecedingSucceedingTitles(instanceUpdateFuture.result(), context))
        .onComplete(ar -> {
          if (ar.succeeded()) {
            future.complete(instanceUpdateFuture.result());
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

  private static Set<String> retrieveTitlesIds(List<JsonObject> precedingSucceedingTitles) {
    return precedingSucceedingTitles.stream()
      .map(titleJson -> titleJson.getString("id"))
      .collect(Collectors.toSet());
  }
}
