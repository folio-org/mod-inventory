package org.folio.inventory.services;

import io.vertx.core.Future;

public interface EventIdStorageService {

  Future<String> store(String eventId, String tenantId);
}
