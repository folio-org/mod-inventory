package org.folio.inventory.dataimport.handlers.actions;

import static org.folio.ActionProfile.FolioRecord.AUTHORITY;
import static org.folio.ActionProfile.FolioRecord.MARC_AUTHORITY;
import static org.folio.DataImportEventTypes.DI_INVENTORY_AUTHORITY_CREATED;

import io.vertx.core.Future;
import io.vertx.core.Promise;

import org.folio.ActionProfile;
import org.folio.Authority;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.storage.Storage;

public class CreateAuthorityEventHandler extends AbstractAuthorityEventHandler {

  protected static final String FAILED_CREATING_AUTHORITY_MSG_TEMPLATE =
    "Failed creating Authority. Cause: %s, status: '%s'";

  public CreateAuthorityEventHandler(Storage storage, MappingMetadataCache mappingMetadataCache) {
    super(storage, mappingMetadataCache);
  }

  @Override
  protected Future<Authority> processAuthority(Authority authority, AuthorityRecordCollection authorityCollection) {
    Promise<Authority> promise = Promise.promise();
    authorityCollection.add(authority, success -> promise.complete(success.getResult()),
      failure -> {
        LOGGER.error(String.format(FAILED_CREATING_AUTHORITY_MSG_TEMPLATE, failure.getReason(), failure.getStatusCode()));
        promise.fail(failure.getReason());
      });
    return promise.future();
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

}
