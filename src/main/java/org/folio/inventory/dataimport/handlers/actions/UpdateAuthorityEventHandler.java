package org.folio.inventory.dataimport.handlers.actions;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.http.HttpStatus;
import org.folio.ActionProfile;
import org.folio.Authority;
import org.folio.DataImportEventPayload;
import org.folio.dbschema.ObjectMapperTool;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.exceptions.DataImportException;
import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.storage.Storage;
import org.folio.processing.events.services.publisher.KafkaEventPublisher;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.ProfileType;

import static java.lang.String.format;
import static org.folio.ActionProfile.Action.UPDATE;
import static org.folio.ActionProfile.FolioRecord.AUTHORITY;
import static org.folio.ActionProfile.FolioRecord.MARC_AUTHORITY;
import static org.folio.DataImportEventTypes.DI_INVENTORY_AUTHORITY_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_AUTHORITY_UPDATED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;

public class UpdateAuthorityEventHandler extends AbstractAuthorityEventHandler {
  private static final int MAX_RETRIES_COUNT = Integer.parseInt(System.getenv().getOrDefault("inventory.di.ol.retry.number", "1"));
  private static final String CURRENT_RETRY_NUMBER = "CURRENT_RETRY_NUMBER";
  private static final String CURRENT_EVENT_TYPE_PROPERTY = "CURRENT_EVENT_TYPE";
  private static final String CURRENT_AUTHORITY_PROPERTY = "CURRENT_AUTHORITY";
  private static final String CURRENT_NODE_PROPERTY = "CURRENT_NODE";
  private static final String SOURCE_CONSORTIUM_PREFIX = "CONSORTIUM-";
  private static final String ERROR_UPDATING_AUTHORITY_MSG_TEMPLATE = "Failed updating Authority. Cause: %s, status: '%s'";
  private static final String AUTHORITY_NOT_FOUND_MSG = "Authority record was not found by id: %s";
  private final KafkaEventPublisher eventPublisher;


  public UpdateAuthorityEventHandler(Storage storage, MappingMetadataCache mappingMetadataCache,
                                     KafkaEventPublisher publisher) {
    super(storage, mappingMetadataCache);
    this.eventPublisher = publisher;
  }

  @Override
  protected Future<Authority> processAuthority(Authority authority,
                                               AuthorityRecordCollection authorityCollection,
                                               DataImportEventPayload payload) {
    return updateAuthority(payload, authority, authorityCollection);
  }

  @Override
  protected String nextEventType() {
    return DI_INVENTORY_AUTHORITY_UPDATED.value();
  }

  @Override
  protected ActionProfile.Action profileAction() {
    return UPDATE;
  }

  @Override
  protected ActionProfile.FolioRecord sourceRecordType() {
    return MARC_AUTHORITY;
  }

  @Override
  protected ActionProfile.FolioRecord targetRecordType() {
    return AUTHORITY;
  }

  @Override
  protected ProfileType profileContentType() {
    return MAPPING_PROFILE;
  }

  @Override
  protected void publishEvent(DataImportEventPayload payload) {
    eventPublisher.publish(payload);
  }

  @Override
  public boolean isPostProcessingNeeded() {
    return false;
  }

  @Override
  public String getPostProcessingInitializationEventType() {
    return DI_INVENTORY_AUTHORITY_UPDATED_READY_FOR_POST_PROCESSING.value();
  }

  @Override
  protected void prepareEvent(DataImportEventPayload payload) {
    super.prepareEvent(payload);
    payload.getContext().put(CURRENT_EVENT_TYPE_PROPERTY, payload.getEventType());
    payload.getContext().put(CURRENT_NODE_PROPERTY, Json.encode(payload.getCurrentNode()));
    payload.getContext().put(CURRENT_AUTHORITY_PROPERTY, Json.encode(payload.getContext().get(AUTHORITY.value())));
  }

  private Future<Authority> updateAuthority(DataImportEventPayload payload, Authority mappedRecord, AuthorityRecordCollection collection) {
    Promise<Authority> promise = Promise.promise();
    collection.findById(mappedRecord.getId()).thenAccept(actualRecord -> {
      if (actualRecord == null) {
        promise.fail(new EventProcessingException(format(AUTHORITY_NOT_FOUND_MSG, mappedRecord.getId())));
        return;
      }
      if (actualRecord.getSource() != null && actualRecord.getSource().value().startsWith(SOURCE_CONSORTIUM_PREFIX)) {
        promise.fail(new DataImportException("Shared MARC authority record cannot be updated from this tenant"));
        return;
      }

      collection.update(
        mappedRecord.withVersion(actualRecord.getVersion()),
        success -> promise.complete(mappedRecord),
        failure -> failureUpdateHandler(payload, mappedRecord.getId(), collection, promise, failure));
    }).exceptionally(e -> {
      String msg = format("Cannot get Authority by id '%s' for jobExecutionId '%s'. Error: %s",
        mappedRecord.getId(), payload.getJobExecutionId(), e.getMessage());
      LOGGER.error(msg, e);
      promise.fail(new EventProcessingException(msg));
      return null;
    });
    return promise.future();
  }

  private void failureUpdateHandler(DataImportEventPayload payload, String id, AuthorityRecordCollection collection, Promise<Authority> promise, Failure failure) {
    if (failure.getStatusCode() == HttpStatus.SC_CONFLICT) {
        processOLError(failure, payload, id, collection, promise);
      } else {
        String msg = format(ERROR_UPDATING_AUTHORITY_MSG_TEMPLATE, failure.getReason(), failure.getStatusCode());
        LOGGER.warn(msg);
        promise.fail(new DataImportException(msg));
      }
  }

  /**
   * This method handles the Optimistic Locking error.
   * The Optimistic Locking error occurs when the updating record has matched and has not updated yet.
   * In this time some another request wants to update the matched record. This happens rarely, however it has a place to be.
   * In such case the method retries a record update:
   * - it calls this 'UpdateAuthorityEventHandler' again keeping a number of calls(retries) in the event context;
   * - when a number of retries exceeded the maximum limit (see <>MAX_RETRIES_COUNT</>) then event handling goes ahead as usual.
   */
  private void processOLError(Failure failure, DataImportEventPayload payload, String recordId, AuthorityRecordCollection recordCollection, Promise<Authority> promise) {
    int currentRetryNumber = payload.getContext().get(CURRENT_RETRY_NUMBER)  == null ? 0 : Integer.parseInt(payload.getContext().get(CURRENT_RETRY_NUMBER));
    if (currentRetryNumber < MAX_RETRIES_COUNT) {
      payload.getContext().put(CURRENT_RETRY_NUMBER, String.valueOf(currentRetryNumber + 1));
      LOGGER.warn("Error updating Authority by id '{}' - '{}', status code '{}'. Retry UpdateAuthorityEventHandler handler...", recordId, failure.getReason(), failure.getStatusCode());
      recordCollection.findById(recordId)
        .thenAccept(actualRecord -> prepareDataAndReInvokeCurrentHandler(payload, promise, actualRecord))
        .exceptionally(e -> {
          payload.getContext().remove(CURRENT_RETRY_NUMBER);
          String errMessage = format("Cannot get actual Authority by id: '%s' for jobExecutionId '%s'. Error: %s ", recordId, payload.getJobExecutionId(), e.getMessage());
          LOGGER.error(errMessage);
          promise.fail(new EventProcessingException(errMessage));
          return null;
        });
    } else {
      payload.getContext().remove(CURRENT_RETRY_NUMBER);
      String errMessage = format("Current retry number %s exceeded or equal given number %s for the Authority update for jobExecutionId '%s' ", MAX_RETRIES_COUNT, currentRetryNumber, payload.getJobExecutionId());
      LOGGER.error(errMessage);
      promise.fail(new EventProcessingException(errMessage));
    }
  }

  private void prepareDataAndReInvokeCurrentHandler(DataImportEventPayload payload, Promise<Authority> promise, Authority actualAuthority) {
    payload.getContext().put(AUTHORITY.value(), Json.encode(JsonObject.mapFrom(actualAuthority)));
    payload.getEventsChain().remove(payload.getContext().get(CURRENT_EVENT_TYPE_PROPERTY));
    try {
      payload.setCurrentNode(ObjectMapperTool.getMapper().readValue(payload.getContext().get(CURRENT_NODE_PROPERTY), ProfileSnapshotWrapper.class));
    } catch (JsonProcessingException e) {
      LOGGER.error(format("Cannot map from CURRENT_NODE value %s", e.getCause()));
    }
    payload.getContext().remove(CURRENT_EVENT_TYPE_PROPERTY);
    payload.getContext().remove(CURRENT_NODE_PROPERTY);
    payload.getContext().remove(CURRENT_AUTHORITY_PROPERTY);
    handle(payload).whenComplete((res, e) -> {
      if (e != null) {
        promise.fail(new EventProcessingException(e.getMessage()));
      } else {
        promise.complete(actualAuthority);
      }
    });
  }
}
