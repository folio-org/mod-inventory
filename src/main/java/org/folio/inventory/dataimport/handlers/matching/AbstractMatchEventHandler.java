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
    if (dataImportEventPayload.getEventType() != null) {
      dataImportEventPayload.getEventsChain().add(dataImportEventPayload.getEventType());
    }
    dataImportEventPayload.getCurrentNode()
      .setContent(new JsonObject((LinkedHashMap) dataImportEventPayload.getCurrentNode().getContent()).mapTo(MatchProfile.class));
    boolean matched = MatchingManager.match(dataImportEventPayload);
    if (matched) {
      dataImportEventPayload.setEventType(getMatchedEventType());
    } else {
      dataImportEventPayload.setEventType(getNotMatchedEventType());
    }
    return CompletableFuture.completedFuture(dataImportEventPayload);
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null && MATCH_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      MatchProfile matchProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(MatchProfile.class);
      return matchProfile.getExistingRecordType() == getEntityType();
    }
    return false;
  }

  abstract EntityType getEntityType();

  abstract String getMatchedEventType();

  abstract String getNotMatchedEventType();

}
