package org.folio.inventory.services;

import io.vertx.core.Future;
import org.folio.inventory.domain.relationship.RecordToEntity;

public interface IdStorageService {

  /**
   * Stores relationship between record and entity
   *
   * @param recordId Record id.
   * @param entityId Entity id.
   * @return future with {@link RecordToEntity}.
   * Returns succeeded future with null if the entity does not exist by the given recordId and entityId.
   */
  Future<RecordToEntity> store(String recordId, String entityId, String tenantId);

}
