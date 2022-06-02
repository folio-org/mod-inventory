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

import static java.lang.String.format;
import static org.folio.ActionProfile.Action.UPDATE;
import static org.folio.ActionProfile.FolioRecord.AUTHORITY;
import static org.folio.ActionProfile.FolioRecord.MARC_AUTHORITY;
import static org.folio.DataImportEventTypes.DI_INVENTORY_AUTHORITY_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_AUTHORITY_UPDATED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;

public class UpdateAuthorityEventHandler extends AbstractAuthorityEventHandler {
  private static final int MAX_RETRIES_COUNT = Integer.parseInt(System.getenv().getOrDefault("inventory.di.ol.retry.number", "1"));
  private static final String CURRENT_RETRY_NUMBER = "CURRENT_RETRY_NUMBER";
  private static final String CURRENT_EVENT_TYPE_PROPERTY = "CURRENT_EVENT_TYPE";
  private static final String CURRENT_AUTHORITY_PROPERTY = "CURRENT_AUTHORITY";
  private static final String CURRENT_NODE_PROPERTY = "CURRENT_NODE";
  protected static final String ERROR_UPDATING_AUTHORITY_MSG_TEMPLATE = "Failed updating Authority. Cause: %s, status: '%s'";
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
  protected ProfileSnapshotWrapper.ContentType profileContentType() {
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

  private Future<Authority> updateAuthority(DataImportEventPayload payload, Authority authority, AuthorityRecordCollection authorityRecordCollection) {
    Promise<Authority> promise = Promise.promise();
    authorityRecordCollection.update(authority, success -> promise.complete(authority), failure -> {
      if (failure.getStatusCode() == HttpStatus.SC_CONFLICT) {
        processOLError(failure, payload, authority.getId(), authorityRecordCollection, promise);
      } else {
        promise.fail(new DataImportException(format(ERROR_UPDATING_AUTHORITY_MSG_TEMPLATE, failure.getReason(), failure.getStatusCode())));
      }
    });
    return promise.future();
  }

  private void processOLError(Failure failure, DataImportEventPayload payload, String recordId, AuthorityRecordCollection recordCollection, Promise<Authority> promise) {
    int currentRetryNumber = payload.getContext().get(CURRENT_RETRY_NUMBER)  == null ? 0 : Integer.parseInt(payload.getContext().get(CURRENT_RETRY_NUMBER));
    if (currentRetryNumber < MAX_RETRIES_COUNT) {
      payload.getContext().put(CURRENT_RETRY_NUMBER, String.valueOf(currentRetryNumber + 1));
      LOGGER.warn("Error updating Authority by id '{}' - '{}', status code '{}'. Retry UpdateAuthorityEventHandler handler...", recordId, failure.getReason(), failure.getStatusCode());
      recordCollection.findById(recordId)
        .thenAccept(actualRecord -> prepareDataAndReInvokeCurrentHandler(payload, promise, actualRecord))
        .exceptionally(e -> {
          payload.getContext().remove(CURRENT_RETRY_NUMBER);
          String errMessage = format("Cannot get actual Authority by id: '%s' for jobExecutionId '%s'. Error: %s ", recordId, payload.getJobExecutionId(), e.getCause());
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
