package org.folio.inventory.dataimport.handlers.matching;

import io.vertx.core.json.JsonObject;
import org.folio.DataImportEventPayload;
import org.folio.MatchProfile;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventHandlerNotFoundException;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static org.folio.rest.jaxrs.model.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.ITEM;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MATCH_PROFILE;

public class CommonMatchProfileEventHandler implements EventHandler {

  private static final String INSTANCES_IDS_KEY = "INSTANCES_IDS";

  private final List<EventHandler> eventHandlers;

  public CommonMatchProfileEventHandler(List<EventHandler> eventHandlers) {
    this.eventHandlers = eventHandlers;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload eventPayload) {
    Optional<EventHandler> handlerOptional = eventHandlers
      .stream()
      .filter(handler -> handler.isEligible(eventPayload))
      .findFirst();

    if (handlerOptional.isPresent()) {
      return handlerOptional.get().handle(eventPayload)
        .thenCompose(dataImportEventPayload -> invokeNextHandlerIfNeeded(dataImportEventPayload));
    } else {
      return CompletableFuture.failedFuture(new EventHandlerNotFoundException(format("No suitable handler found for %s event type", eventPayload.getEventType())));
    }
  }

  private CompletableFuture<DataImportEventPayload> invokeNextHandlerIfNeeded(DataImportEventPayload eventPayload) {
    if (!isMultipleMatchResult(eventPayload)) {
      return CompletableFuture.completedFuture(eventPayload);
    }

    Optional<ProfileSnapshotWrapper> nextProfileOptional = extractNextProfile(eventPayload);

    if (nextProfileOptional.isPresent() && nextProfileOptional.get().getContentType() == MATCH_PROFILE) {
      eventPayload.setCurrentNode(nextProfileOptional.get());
      Optional<EventHandler> optionalEventHandler = eventHandlers.stream()
        .filter(eventHandler -> eventHandler.isEligible(eventPayload))
        .findFirst();

      if (optionalEventHandler.isPresent()) {
        return optionalEventHandler.get().handle(eventPayload)
          .thenCompose(this::invokeNextHandlerIfNeeded);
      }
      return CompletableFuture.failedFuture(new EventHandlerNotFoundException(format("No suitable handler found for %s event type", eventPayload.getEventType())));
    }
    return CompletableFuture.failedFuture(new EventProcessingException("Multiple entities have been matched"));
  }

  private Optional<ProfileSnapshotWrapper> extractNextProfile(DataImportEventPayload eventPayload) {
    return eventPayload.getCurrentNode().getChildSnapshotWrappers()
      .stream()
      .filter(child -> child.getReactTo() == ProfileSnapshotWrapper.ReactTo.MATCH
        && child.getOrder() == 0 && child.getContentType() == MATCH_PROFILE)
      .findFirst();
  }

  private boolean isMultipleMatchResult(DataImportEventPayload dataImportEventPayload) {
    return !dataImportEventPayload.getEventType().endsWith("NOT_MATCHED")
      && dataImportEventPayload.getContext().containsKey(INSTANCES_IDS_KEY);
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode() != null
      && MATCH_PROFILE == dataImportEventPayload.getCurrentNode().getContentType()) {
      var matchProfile = JsonObject.mapFrom(dataImportEventPayload.getCurrentNode().getContent()).mapTo(MatchProfile.class);

      return matchProfile.getExistingRecordType() == MARC_BIBLIOGRAPHIC
        || matchProfile.getExistingRecordType() == INSTANCE
        || matchProfile.getExistingRecordType() == HOLDINGS
        || matchProfile.getExistingRecordType() == ITEM;
    }
    return false;
  }

}
