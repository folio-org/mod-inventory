package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.ActionProfile;
import org.folio.Authority;
import org.folio.DataImportEventPayload;
import org.folio.inventory.dataimport.exceptions.DataImportException;
import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.storage.Storage;
import org.folio.processing.events.services.handler.EventHandler;
import org.folio.processing.exceptions.EventProcessingException;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;

import java.util.concurrent.CompletableFuture;

import static io.vertx.core.json.JsonObject.mapFrom;
import static java.lang.String.format;
import static org.folio.ActionProfile.Action.DELETE;
import static org.folio.ActionProfile.FolioRecord.MARC_AUTHORITY;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.constructContext;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;

public class DeleteAuthorityEventHandler implements EventHandler {
  private static final Logger LOGGER = LogManager.getLogger(DeleteAuthorityEventHandler.class);

  private static final String MARC_AUTHORITY_ID = "MARC_AUTHORITY_ID";
  private static final String UNEXPECTED_PAYLOAD_MSG = "Unexpected payload";
  private static final String ERROR_DELETING_AUTHORITY_MSG_TEMPLATE = "Failed deleting Authority. Cause: %s, status: '%s'";
  private static final String DELETE_FAILED_MSG_PATTERN = "Action DELETE for AUTHORITY record failed.";
  private static final String DELETE_SUCCEED_MSG_PATTERN = "Action DELETE for AUTHORITY record succeed.";
  private static final String META_INFO_MSG_PATTERN = "JobExecutionId: '%s', RecordId: '%s', ChunkId: '%s'";
  private static final String RECORD_ID_HEADER = "recordId";
  private static final String CHUNK_ID_HEADER = "chunkId";

  private final Storage storage;

  public DeleteAuthorityEventHandler(Storage storage) {
    this.storage = storage;
  }

  @Override
  public CompletableFuture<DataImportEventPayload> handle(DataImportEventPayload payload) {
    CompletableFuture<DataImportEventPayload> future = new CompletableFuture<>();
    try {
      if (!isExpectedPayload(payload)) {
        throw new EventProcessingException(UNEXPECTED_PAYLOAD_MSG);
      }

      var context = constructContext(payload.getTenant(), payload.getToken(), payload.getOkapiUrl());
      AuthorityRecordCollection authorityRecordCollection = storage.getAuthorityRecordCollection(context);
      String id = payload.getContext().get(MARC_AUTHORITY_ID);

      deleteAuthorityRecord(id, authorityRecordCollection)
        .onSuccess(successHandler(payload, future))
        .onFailure(failureHandler(payload, future));
    } catch (Exception e) {
      LOGGER.error(constructMsg(DELETE_FAILED_MSG_PATTERN, payload), e);
      future.completeExceptionally(e);
    }

    return future;
  }

  @Override
  public boolean isEligible(DataImportEventPayload payload) {
    if (isExpectedPayload(payload)) {
      ProfileSnapshotWrapper currentNode = payload.getCurrentNode();
      if (currentNode != null && ACTION_PROFILE == currentNode.getContentType()) {
        var actionProfile = mapFrom(currentNode.getContent()).mapTo(ActionProfile.class);
        return DELETE == actionProfile.getAction() && MARC_AUTHORITY == actionProfile.getFolioRecord();
      }
    }

    return false;
  }

  private boolean isExpectedPayload(DataImportEventPayload payload) {
    return payload != null
      && payload.getContext() != null
      && !payload.getContext().isEmpty()
      && StringUtils.isNotBlank(payload.getContext().get(MARC_AUTHORITY_ID));
  }

  private Future<Authority> deleteAuthorityRecord(String id, AuthorityRecordCollection authorityCollection) {
    Promise<Authority> promise = Promise.promise();
    authorityCollection.delete(id, success -> promise.complete(),
      failure -> promise.fail(new DataImportException(format(ERROR_DELETING_AUTHORITY_MSG_TEMPLATE,
        failure.getReason(), failure.getStatusCode()))
      ));
    return promise.future();
  }

  private Handler<Authority> successHandler(DataImportEventPayload payload,
                                            CompletableFuture<DataImportEventPayload> future) {
    return authority -> {
      LOGGER.info(constructMsg(DELETE_SUCCEED_MSG_PATTERN, payload));
      future.complete(payload);
    };
  }

  private Handler<Throwable> failureHandler(DataImportEventPayload payload,
                                            CompletableFuture<DataImportEventPayload> future) {
    return e -> {
      LOGGER.error(constructMsg(DELETE_FAILED_MSG_PATTERN, payload), e);
      future.completeExceptionally(e);
    };
  }

  private String constructMsg(String message, DataImportEventPayload payload) {
    if (payload == null) {
      return message;
    } else {
      return message + " " + format(
        META_INFO_MSG_PATTERN,
        payload.getJobExecutionId(),
        getDataIdHeader(payload, RECORD_ID_HEADER),
        getDataIdHeader(payload, CHUNK_ID_HEADER)
      );
    }
  }

  private String getDataIdHeader(DataImportEventPayload payload, String key) {
    return payload.getContext() == null ? "-" : payload.getContext().get(key);
  }

}
