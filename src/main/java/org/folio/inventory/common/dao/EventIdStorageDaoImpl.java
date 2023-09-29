package org.folio.inventory.common.dao;

import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.domain.relationship.EventTable;
import org.folio.inventory.domain.relationship.EventToEntity;

import java.util.UUID;

public class EventIdStorageDaoImpl implements EventIdStorageDao {
  private static final Logger LOGGER = LogManager.getLogger(EventIdStorageDaoImpl.class);

  private static final String INSERT_FUNCTION = "INSERT INTO {schemaName}.{tableName} VALUES ($1::uuid, current_timestamp) RETURNING *;";

  private final PostgresClientFactory postgresClientFactory;

  public EventIdStorageDaoImpl(final PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
  }

  @Override
  public Future<String> storeEvent(EventToEntity eventToEntity, String tenantId) {
    EventTable eventTable = eventToEntity.getTable();
    UUID eventId = UUID.fromString(eventToEntity.getEventId());
    String tableName = eventTable.getTableName();

    LOGGER.info("Trying to save event to {} with eventId = {}", tableName, eventId);
    String sql = prepareQuery(eventTable);
    Tuple tuple = Tuple.of(eventId);

    return postgresClientFactory.execute(sql, tuple, tenantId)
      .map(this::retrieveEventId);
  }

  private String retrieveEventId(RowSet<Row> rows) {
    Row row = rows.iterator().next();
    return String.valueOf(row.getValue("event_id"));
  }

  private String prepareQuery(EventTable eventTable) {
    return INSERT_FUNCTION.replace("{tableName}", eventTable.getTableName());
  }
}
