package org.folio.inventory.services;

import io.vertx.core.Future;
import org.folio.inventory.domain.relationship.RecordToEntity;

import java.util.Optional;

public interface IdStorageService {

  /**
   * Stores relationship between record and entity
   *
   * @param recordId Record id
   * @param entityId Entity id
   * @return future with Optional of Entity.
   * Returns succeeded future with an empty Optional if the entity does not exist by the given
   */
  Future<Optional<RecordToEntity>> store(String recordId, String entityId, String tenantId);

}
