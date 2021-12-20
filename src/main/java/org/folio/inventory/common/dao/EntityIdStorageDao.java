package org.folio.inventory.common.dao;

import io.vertx.core.Future;
import org.folio.inventory.domain.relationship.RecordToEntity;

import java.util.Optional;

/**
 * Data access object for Entity
 */
public interface EntityIdStorageDao {

  /**
   * Saves Entity to database
   *
   * @param record  to save
   * @param tenantId tenant id
   * @return future with saved entity
   */
  Future<Optional<RecordToEntity>> saveRecordToEntityRelationship(RecordToEntity record, String tenantId);

}
