package org.folio.inventory.services;

import io.vertx.core.Future;

/**
 * Service for event storage
 */
public interface EventIdStorageService {

  /**
   * Store event.
   * If there is already an event in DB with the same eventId - it will return DuplicateEventException.
   * @param eventId - eventId
   * @param tenantId - tenantId
   * @return - future with saved eventId
   */
  Future<String> store(String eventId, String tenantId);
}
