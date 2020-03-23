package org.folio.inventory.dataimport.handlers.matching;

import io.vertx.core.json.JsonObject;
import org.folio.DataImportEventPayload;
import org.folio.MatchProfile;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.matching.MatchingManager;
import org.folio.rest.jaxrs.model.EntityType;

import java.util.LinkedHashMap;
import java.util.concurrent.CompletableFuture;

import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MATCH_PROFILE;

public abstract class AbstractMatchEventHandler implements EventHandler {

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload dataImportEventPayload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());
      dataImportEventPayload.getCurrentNode()
        .setContent(new JsonObject((LinkedHashMap) dataImportEventPayload.getCurrentNode().getContent()).mapTo(MatchProfile.class));
      boolean matched = MatchingManager.match(dataImportEventPayload);
      if (matched) {
        dataImportEventPayload.setEventType(getMatchedEventType());
      } else {
        dataImportEventPayload.setEventType(getNotMatchedEventType());
      }
      future.complete(dataImportEventPayload);
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null && MATCH_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      MatchProfile matchProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(MatchProfile.class);
      return matchProfile.getExistingRecordType() == getEntityType();
    }
    return false;
  }

  protected abstract EntityType getEntityType();

  protected abstract String getMatchedEventType();

  protected abstract String getNotMatchedEventType();

}
