package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.Context;
import org.folio.inventory.domain.instances.Instance;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.util.OkapiConnectionParams;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.inventory.dataimport.util.EventHandlingUtil.sendEventWithPayload;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


public class UpdateInstanceEventHandler {

  private static final Logger LOGGER = LogManager.getLogger(UpdateInstanceEventHandler.class);
  private static final String MARC_KEY = "MARC";
  private static final String USER_CONTEXT_KEY = "USER_CONTEXT";
  private static final String MAPPING_RULES_KEY = "MAPPING_RULES";
  private static final String MAPPING_PARAMS_KEY = "MAPPING_PARAMS";
  private static final String DI_INDICATOR = "DI";

  private static final String TOKEN_KEY = "token";
  private static final String USER_ID_KEY = "userId";

  private final Context context;
  private final InstanceUpdateDelegate instanceUpdateDelegate;
  private final PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper;

  public UpdateInstanceEventHandler(InstanceUpdateDelegate updateInstanceDelegate, Context context,
                                    PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper) {
    this.context = context;
    this.instanceUpdateDelegate = updateInstanceDelegate;
    this.precedingSucceedingTitlesHelper = precedingSucceedingTitlesHelper;
  }

  public CompletableFuture<Instance> handle(Map<String, String> eventPayload, Map<String, String> requestHeaders, Vertx vertx) {
    CompletableFuture<Instance> future = new CompletableFuture<>();
    try {
      if (eventPayload == null || isEmpty(eventPayload.get(MARC_KEY)) || isEmpty(eventPayload.get(MAPPING_RULES_KEY)) || isEmpty(eventPayload.get(MAPPING_PARAMS_KEY)) || isEmpty(eventPayload.get(USER_CONTEXT_KEY))) {
        String message = "Event does not contain required data to update Instance";
        LOGGER.error(message);
        future.completeExceptionally(new EventProcessingException(message));
        return future;
      }

      var userContext = new JsonObject(eventPayload.get(USER_CONTEXT_KEY));

      Record marcRecord = new JsonObject(eventPayload.get(MARC_KEY)).mapTo(Record.class);
      Future<Instance> instanceUpdateFuture = instanceUpdateDelegate.handle(eventPayload, marcRecord, getUpdateContext(userContext));

      instanceUpdateFuture
        .compose(updatedInstance -> precedingSucceedingTitlesHelper.getExistingPrecedingSucceedingTitles(updatedInstance, context))
        .map(UpdateInstanceEventHandler::retrieveTitlesIds)
        .compose(titlesIds -> precedingSucceedingTitlesHelper.deletePrecedingSucceedingTitles(titlesIds, context))
        .compose(ar -> precedingSucceedingTitlesHelper.createPrecedingSucceedingTitles(instanceUpdateFuture.result(), context))
        .onComplete(ar -> {
          if (isEmpty(eventPayload.get((DI_INDICATOR)))) {
            OkapiConnectionParams params = new OkapiConnectionParams(requestHeaders, vertx);
            if (ar.succeeded()) {
              sendEventWithPayload(Json.encode(eventPayload), "QM_INVENTORY_INSTANCE_UPDATED", params)
                .onComplete(v -> future.complete(instanceUpdateFuture.result()));
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

  private Context getUpdateContext(JsonObject userContext) {
    return new Context() {

      @Override
      public String getTenantId() {
        return UpdateInstanceEventHandler.this.context.getTenantId();
      }

      @Override
      public String getToken() {
        return userContext.getString(TOKEN_KEY);
      }

      @Override
      public String getOkapiLocation() {
        return UpdateInstanceEventHandler.this.context.getOkapiLocation();
      }

      @Override public String getUserId() {
        return userContext.getString(USER_ID_KEY);
      }
    };
  }

  private static Set<String> retrieveTitlesIds(List<JsonObject> precedingSucceedingTitles) {
    return precedingSucceedingTitles.stream()
      .map(titleJson -> titleJson.getString("id"))
      .collect(Collectors.toSet());
  }
}
