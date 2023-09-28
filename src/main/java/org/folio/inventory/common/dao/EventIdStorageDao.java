package org.folio.inventory.common.dao;

import io.vertx.core.Future;
import org.folio.inventory.domain.relationship.EventToEntity;

public interface EventIdStorageDao {

  Future<String> storeEvent(EventToEntity eventToEntity, String tenantId);
}
