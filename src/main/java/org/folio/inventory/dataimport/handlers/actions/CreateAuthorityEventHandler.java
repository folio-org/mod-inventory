package org.folio.inventory.dataimport.handlers.actions;

import static org.folio.ActionProfile.FolioRecord.AUTHORITY;
import static org.folio.ActionProfile.FolioRecord.MARC_AUTHORITY;
import static org.folio.DataImportEventTypes.DI_INVENTORY_AUTHORITY_CREATED;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;

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
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;

public class CreateAuthorityEventHandler extends AbstractAuthorityEventHandler {

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
        LOGGER.error(String.format(FAILED_CREATING_AUTHORITY_MSG_TEMPLATE, failure.getReason(), failure.getStatusCode()));
        promise.fail(failure.getReason());
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
  protected ProfileSnapshotWrapper.ContentType profileContentType() {
    return ACTION_PROFILE;
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
