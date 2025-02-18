package org.folio.inventory.dataimport.handlers.actions;

import static java.lang.String.format;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.folio.ActionProfile.FolioRecord.AUTHORITY;
import static org.folio.ActionProfile.FolioRecord.MARC_AUTHORITY;
import static org.folio.DataImportEventTypes.DI_INVENTORY_AUTHORITY_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_AUTHORITY_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;

import io.vertx.core.Future;
import io.vertx.core.Promise;

import org.folio.ActionProfile;
import org.folio.Authority;
import org.folio.DataImportEventPayload;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.domain.relationship.RecordToEntity;
import org.folio.inventory.services.IdStorageService;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.rest.jaxrs.model.ProfileType;

public class CreateAuthorityEventHandler extends AbstractAuthorityEventHandler {

  static final String ID_UNIQUENESS_ERROR = "Authority with the given 'id' already exists";
  protected static final String FAILED_CREATING_AUTHORITY_MSG_TEMPLATE =
    "Failed creating Authority. Cause: %s, status: '%s'";
  private static final String CREATING_RELATIONSHIP_ERROR =
    "Error creating inventory recordId and authorityId relationship";

  private final IdStorageService idStorageService;

  public CreateAuthorityEventHandler(Storage storage,
                                     MappingMetadataCache mappingMetadataCache,
                                     IdStorageService idStorageService) {
    super(storage, mappingMetadataCache);
    this.idStorageService = idStorageService;
  }

  @Override
  protected Future<Authority> processAuthority(Authority authority,
                                               AuthorityRecordCollection authorityCollection,
                                               DataImportEventPayload payload) {
    Promise<Authority> promise = Promise.promise();
    createRelationship(promise, authority, payload);
    authorityCollection.add(authority, success -> promise.complete(success.getResult()),
      failure -> {
        //This is temporary solution (verify by error message). It will be improved via another solution by https://issues.folio.org/browse/RMB-899.
        if (isNotBlank(failure.getReason()) && failure.getReason().contains(ID_UNIQUENESS_ERROR)) {
          LOGGER.info("Duplicated event received by AuthorityId: {}. Ignoring...", authority.getId());
          promise.fail(new DuplicateEventException(format("Duplicated event by Authority id: %s", authority.getId())));
        } else {
          LOGGER.error(String.format(FAILED_CREATING_AUTHORITY_MSG_TEMPLATE, failure.getReason(), failure.getStatusCode()));
          promise.fail(failure.getReason());
        }
      });
    return promise.future();
  }

  @Override
  protected void prepareEvent(DataImportEventPayload payload) {
    super.prepareEvent(payload);
    payload.setCurrentNode(payload.getCurrentNode().getChildSnapshotWrappers().get(0));
  }

  @Override
  protected String nextEventType() {
    return DI_INVENTORY_AUTHORITY_CREATED.value();
  }

  @Override
  protected ActionProfile.Action profileAction() {
    return ActionProfile.Action.CREATE;
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
    return ACTION_PROFILE;
  }

  @Override
  protected void publishEvent(DataImportEventPayload payload) {

  }

  @Override
  public String getPostProcessingInitializationEventType() {
    return DI_INVENTORY_AUTHORITY_CREATED_READY_FOR_POST_PROCESSING.value();
  }

  private void createRelationship(Promise<Authority> promise, Authority authority, DataImportEventPayload payload) {
    Future<RecordToEntity> recordToAuthorityFuture = idStorageService.store(getRecordIdHeader(payload), authority.getId(), payload.getTenant());
    recordToAuthorityFuture
      .onFailure(failure -> {
        LOGGER.error(constructMsg(CREATING_RELATIONSHIP_ERROR, payload), failure);
        promise.fail(failure);
      });
  }
}
