package org.folio.inventory.dataimport.handlers.actions;

import static java.lang.String.format;

import static org.folio.ActionProfile.Action.UPDATE;
import static org.folio.ActionProfile.FolioRecord.AUTHORITY;
import static org.folio.ActionProfile.FolioRecord.MARC_AUTHORITY;
import static org.folio.DataImportEventTypes.DI_INVENTORY_AUTHORITY_UPDATED;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;

import io.vertx.core.Future;
import io.vertx.core.Promise;

import org.folio.ActionProfile;
import org.folio.Authority;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.exceptions.DataImportException;
import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.storage.Storage;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;

public class UpdateAuthorityEventHandler extends AbstractAuthorityEventHandler {

  protected static final String ERROR_UPDATING_AUTHORITY_MSG_TEMPLATE = "Failed updating Authority. Cause: %s, status: '%s'";

  public UpdateAuthorityEventHandler(Storage storage, MappingMetadataCache mappingMetadataCache) {
    super(storage, mappingMetadataCache);
  }

  @Override
  protected Future<Authority> processAuthority(Authority authority, AuthorityRecordCollection authorityCollection) {
    return updateAuthority(authority, authorityCollection);
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

  private Future<Authority> updateAuthority(Authority authority, AuthorityRecordCollection authorityRecordCollection) {
    Promise<Authority> promise = Promise.promise();
    authorityRecordCollection.update(authority, success -> promise.complete(authority),
      failure -> promise.fail(new DataImportException(format(ERROR_UPDATING_AUTHORITY_MSG_TEMPLATE, failure.getReason(),
        failure.getStatusCode()))
      ));
    return promise.future();
  }
}
