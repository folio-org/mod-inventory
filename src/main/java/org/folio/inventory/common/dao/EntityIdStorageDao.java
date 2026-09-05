package org.folio.inventory.common.dao;

import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.inventory.domain.relationship.EntityTable;
import org.folio.inventory.domain.relationship.RecordToEntity;

public class EntityIdStorageDao {

  private static final Logger LOGGER = LogManager.getLogger(EntityIdStorageDao.class);

  private static final String INSERT_FUNCTION = """
    WITH input_rows({recordIdFieldName}, {entityIdFieldName}) AS (
       VALUES ($1::uuid,$2::uuid)
    )
    , ins AS (
       INSERT INTO {schemaName}.{tableName}({recordIdFieldName}, {entityIdFieldName})
       SELECT * FROM input_rows
       ON CONFLICT ({recordIdFieldName}) DO UPDATE SET {recordIdFieldName}=EXCLUDED.{recordIdFieldName}
       RETURNING {recordIdFieldName}::uuid, {entityIdFieldName}::uuid
       )
    SELECT {recordIdFieldName}, {entityIdFieldName}
    FROM   ins
    UNION  ALL
    SELECT c.{recordIdFieldName}, c.{entityIdFieldName}
    FROM   input_rows
    JOIN   {schemaName}.{tableName} c USING ({recordIdFieldName});
    """;

  private final PostgresClientFactory postgresClientFactory;

  public EntityIdStorageDao(final PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
  }

  public Future<RecordToEntity> saveRecordToEntityRelationship(RecordToEntity recordToEntity, String tenantId) {
    EntityTable entityTable = recordToEntity.getTable();
    UUID recordId = UUID.fromString(recordToEntity.getRecordId());
    UUID entityId = UUID.fromString(recordToEntity.getEntityId());
    String tableName = entityTable.getTableName();

    LOGGER.info("Trying to save entity to {} with recordId = {} and entityId = {}", tableName, recordId, entityId);
    String sql = prepareQuery(entityTable);
    Tuple tuple = Tuple.of(recordId, entityId);

    return postgresClientFactory.execute(sql, tuple, tenantId)
      .map(rows -> mapRowToRecordToEntity(rows, entityTable));
  }

  /**
   * Convert database query result {@link RowSet} to {@link RecordToEntity}.
   * There is no case when DB returns empty RowSet, so hasNext check is not needed yet.
   *
   * @param rows query result RowSet.
   * @return RecordToInstance
   */
  private RecordToEntity mapRowToRecordToEntity(RowSet<Row> rows, EntityTable entityTable) {
    Row row = rows.iterator().next();
    return RecordToEntity.builder()
      .table(entityTable)
      .recordId(row.getValue(entityTable.getRecordIdFieldName()).toString())
      .entityId(row.getValue(entityTable.getEntityIdFieldName()).toString())
      .build();
  }

  /**
   * Prepares SQL query for Insert.
   *
   * @param entityTable the entity table.
   * @return sql query to use.
   */
  private String prepareQuery(EntityTable entityTable) {
    return INSERT_FUNCTION.replace("{recordIdFieldName}", entityTable.getRecordIdFieldName())
      .replace("{entityIdFieldName}", entityTable.getEntityIdFieldName())
      .replace("{tableName}", entityTable.getTableName());
  }
}
