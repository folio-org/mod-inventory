package org.folio.inventory.dataimport.handlers.actions;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.folio.ActionProfile;
import org.folio.Authority;
import org.folio.DataImportEventPayload;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.exceptions.DataImportException;
import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.storage.Storage;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;

import static java.lang.String.format;
import static org.folio.ActionProfile.FolioRecord.AUTHORITY;
import static org.folio.ActionProfile.FolioRecord.MARC_AUTHORITY;
import static org.folio.ActionProfile.Action.DELETE;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_DELETED;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;

public class DeleteAuthorityEventHandler extends AbstractAuthorityEventHandler {
  protected static final String ERROR_DELETING_AUTHORITY_MSG_TEMPLATE = "Failed deleting Authority. Cause: %s, status: '%s'";

  public DeleteAuthorityEventHandler(Storage storage, MappingMetadataCache mappingMetadataCache) {
    super(storage, mappingMetadataCache);
  }

  @Override
  protected Future<Authority> processAuthority(Authority authority, AuthorityRecordCollection authorityCollection, DataImportEventPayload payload) {
    return deleteAuthority(authority, authorityCollection);
  }

  @Override
  protected String nextEventType() {
    return DI_SRS_MARC_AUTHORITY_RECORD_DELETED.value();
  }

  @Override
  protected ActionProfile.Action profileAction() {
    return DELETE;
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

  private Future<Authority> deleteAuthority(Authority authority, AuthorityRecordCollection authorityCollection) {
    Promise<Authority> promise = Promise.promise();
    authorityCollection.delete(authority.getId(), success -> promise.complete(authority),
      failure -> promise.fail(new DataImportException(format(ERROR_DELETING_AUTHORITY_MSG_TEMPLATE, failure.getReason(),
        failure.getStatusCode()))
      ));
    return promise.future();
  }

}
