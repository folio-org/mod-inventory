package org.folio.inventory.dataimport.handlers.matching;

import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.MatchProfile;
import org.folio.processing.events.services.handler.EventHandler;
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
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ReactTo.MATCH;

/**
 * The implementation of the {@link EventHandler} that represents
 * an entry point for initiating instance/holdings/item/MARC-BIB matching processing
 * and is responsible for dispatching event payload to the appropriate dedicated matching event handler.
 * Basically, it is needed to avoid event sending containing multiple marc-bib search results
 * and related overhead, and to proceed with its processing right after marc-bib matching
 * during the next match profile processing.
 */
public class CommonMatchEventHandler implements EventHandler {

  private static final Logger LOG = LogManager.getLogger();

  private static final String INCOMING_RECORD_ID_KEY = "INCOMING_RECORD_ID";
  private static final String INSTANCES_IDS_KEY = "INSTANCES_IDS";
  private static final String NOT_MATCHED_RESULT_EVENT_SUFFIX = "NOT_MATCHED";
  private static final String MULTIPLE_RECORDS_FOUND_MSG = "Found multiple records corresponding to match profile criteria, incomingRecordId: '%s', jobExecutionId: '%s'";

  private final List<EventHandler> eventHandlers;

  public CommonMatchEventHandler(List<EventHandler> eventHandlers) {
    this.eventHandlers = eventHandlers;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload eventPayload) {
    try {
      Optional<EventHandler> handlerOptional = findEligibleHandler(eventPayload);

      if (handlerOptional.isPresent()) {
        return handlerOptional.get().handle(eventPayload)
          .thenCompose(this::invokeNextHandlerIfNeeded);
      } else {
        String msg = format("Event payload with current node '%s' is not supported, incomingRecordId: '%s', jobExecutionId: '%s'",
          eventPayload.getCurrentNode().getContentType(), extractIncomingRecordId(eventPayload), eventPayload.getJobExecutionId());
        LOG.warn("handle:: {}", msg);
        return CompletableFuture.failedFuture(new EventProcessingException(msg));
      }
    } catch (Exception e) {
      LOG.warn("handle:: Error while processing event: '{}' for match profile processing, incomingRecordId: '{}', jobExecutionId: '{}'",
        eventPayload.getEventType(), extractIncomingRecordId(eventPayload), eventPayload.getJobExecutionId(), e);
      return CompletableFuture.failedFuture(new EventProcessingException(e));
    }
  }

  private Optional<EventHandler> findEligibleHandler(DataImportEventPayload eventPayload) {
    return eventHandlers
      .stream()
      .filter(handler -> handler.isEligible(eventPayload))
      .findFirst();
  }

  private CompletableFuture<DataImportEventPayload> invokeNextHandlerIfNeeded(DataImportEventPayload eventPayload) {
    if (!containsMultipleMatchResult(eventPayload)) {
      return CompletableFuture.completedFuture(eventPayload);
    }

    Optional<ProfileSnapshotWrapper> nextProfileOptional = extractNextProfile(eventPayload);

    if (nextProfileOptional.isPresent() && nextProfileOptional.get().getContentType() == MATCH_PROFILE) {
      eventPayload.setCurrentNode(nextProfileOptional.get());
      Optional<EventHandler> optionalEventHandler = findEligibleHandler(eventPayload);
      if (optionalEventHandler.isPresent()) {
        LOG.debug("invokeNextHandlerIfNeeded:: Invoking '{}' event handler, incomingRecordId: '{}', jobExecutionId: '{}'",
          optionalEventHandler.get().getClass().getSimpleName(), extractIncomingRecordId(eventPayload), eventPayload.getJobExecutionId()); //NOSONAR
        return optionalEventHandler.get().handle(eventPayload)
          .thenCompose(this::invokeNextHandlerIfNeeded);
      }

      String msg = format("No suitable handler found to handle multiple match result, eventType: '%s'", eventPayload.getEventType());
      return CompletableFuture.failedFuture(new EventProcessingException(msg));
    }

    String msg = format(MULTIPLE_RECORDS_FOUND_MSG, extractIncomingRecordId(eventPayload), eventPayload.getJobExecutionId());
    LOG.warn("invokeNextHandlerIfNeeded:: {}", msg);
    return CompletableFuture.failedFuture(new EventProcessingException(msg));
  }

  private Optional<ProfileSnapshotWrapper> extractNextProfile(DataImportEventPayload eventPayload) {
    return eventPayload.getCurrentNode().getChildSnapshotWrappers()
      .stream()
      .filter(child -> child.getReactTo() == MATCH
        && child.getOrder() == 0 && child.getContentType() == MATCH_PROFILE)
      .findFirst();
  }

  private boolean containsMultipleMatchResult(DataImportEventPayload eventPayload) {
    return !eventPayload.getEventType().endsWith(NOT_MATCHED_RESULT_EVENT_SUFFIX)
      && eventPayload.getContext().containsKey(INSTANCES_IDS_KEY);
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

  private String extractIncomingRecordId(DataImportEventPayload eventPayload) {
    return eventPayload.getContext().get(INCOMING_RECORD_ID_KEY);
  }

}
