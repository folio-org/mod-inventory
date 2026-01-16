package org.folio.inventory.services;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.pgclient.PgException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.common.dao.EventIdStorageDao;
import org.folio.inventory.domain.relationship.EventTable;
import org.folio.inventory.domain.relationship.EventToEntity;
import org.folio.kafka.exception.DuplicateEventException;

public class SharedInstanceEventIdStorageServiceImpl implements EventIdStorageService {

  private static final Logger LOGGER = LogManager.getLogger(SharedInstanceEventIdStorageServiceImpl.class);

  private static final int UNIQUE_VIOLATION_SQL_STATE = 23505;

  private final EventIdStorageDao eventIdStorageDao;

  public SharedInstanceEventIdStorageServiceImpl(EventIdStorageDao entityIdStorageDao) {
    this.eventIdStorageDao = entityIdStorageDao;
  }

  @Override
  public Future<String> store(String eventId, String tenantId) {
    Promise<String> promise = Promise.promise();
    LOGGER.info("store:: Saving event for shared instanceId: {} tenantId: {}", eventId, tenantId);
    EventToEntity eventToEntity = EventToEntity.builder().table(EventTable.SHARED_INSTANCE).eventId(eventId).build();
    eventIdStorageDao.storeEvent(eventToEntity, tenantId)
      .onSuccess(promise::complete)
      .onFailure(error -> {
        if (error instanceof PgException pgException) {
          if (pgException.getErrorCode() == UNIQUE_VIOLATION_SQL_STATE) {
            promise.fail(new DuplicateEventException("SQL Unique constraint violation prevented repeatedly saving the record"));
          } else {
            promise.fail(error);
          }
        } else {
          promise.fail(error);
        }
      });
    return promise.future();
  }
}
