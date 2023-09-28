package org.folio.inventory.common.dao;

import io.vertx.core.Future;
import org.folio.inventory.domain.relationship.EventToEntity;

/**
 * DAO for manipulation via eventId-based tables.
 */
public interface EventIdStorageDao {

  /**
   * Creates a new record inside specific table (set in the EventToEntity:table) with eventId as a primary key.
   * @param eventToEntity - entity with eventId and table
   * @param tenantId - tenant id
   * @return - future with saved eventId.
   */
  Future<String> storeEvent(EventToEntity eventToEntity, String tenantId);
}
