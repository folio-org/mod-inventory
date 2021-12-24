package org.folio.inventory.common.dao;

import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import org.folio.inventory.domain.relationship.EntityTable;
import org.folio.inventory.domain.relationship.RecordToEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;

public class EntityIdStorageDaoImpl implements EntityIdStorageDao {

  private static final Logger LOGGER = LoggerFactory.getLogger(EntityIdStorageDaoImpl.class);

  private static final String INSERT_FUNCTION = "WITH input_rows({recordIdFieldName}, {entityIdFieldName}) AS (\n" +
    "   VALUES ($1,$2)\n" +
    ")\n" +
    ", ins AS (\n" +
    "   INSERT INTO {schemaName}.{tableName}({recordIdFieldName}, {entityIdFieldName})\n" +
    "   SELECT * FROM input_rows\n" +
    "   ON CONFLICT ({recordIdFieldName}) DO NOTHING\n" +
    "   RETURNING {recordIdFieldName}, {entityIdFieldName}\n" +
    "   )\n" +
    "SELECT {recordIdFieldName}, {entityIdFieldName}\n" +
    "FROM   ins\n" +
    "UNION  ALL\n" +
    "SELECT c.{recordIdFieldName}, c.{entityIdFieldName} \n" +
    "FROM   input_rows\n" +
    "JOIN   {schemaName}.{tableName} c USING ({recordIdFieldName});";

  private final PostgresClientFactory postgresClientFactory;

  public EntityIdStorageDaoImpl(final PostgresClientFactory postgresClientFactory) {
    this.postgresClientFactory = postgresClientFactory;
  }

  @Override
  public Future<Optional<RecordToEntity>> saveRecordToEntityRelationship(RecordToEntity recordToEntity, String tenantId) {
    EntityTable entityTable = recordToEntity.getTable();
    UUID recordId = UUID.fromString(recordToEntity.getRecordId());
    UUID entityId = UUID.fromString(recordToEntity.getEntityId());
    String tableName = entityTable.getTableName();

    LOGGER.info("Trying to save entity to {} with recordId = {} and entityId = {}", tableName, recordId, entityId);
    String sql = prepareQuery(entityTable);
    Tuple tuple = Tuple.of(recordId, entityId);

    return postgresClientFactory.execute(sql, tuple, tenantId)
      .map(rows -> rowSetToOptionalRecordToEntity(rows, entityTable));
  }

  /**
   * Convert database query result {@link Row} to {@link Optional} {@link RecordToEntity}
   *
   * @param rows query RowSet result
   * @return optional RecordToEntity
   */
  private Optional<RecordToEntity> rowSetToOptionalRecordToEntity(RowSet<Row> rows, EntityTable entityTable) {
    return rows.iterator().hasNext() ? Optional.of(mapRowToRecordToEntity(rows.iterator().next(), entityTable)) : Optional.empty();
  }

  /**
   * Convert database query result {@link Row} to {@link RecordToEntity}
   *
   * @param row query result row
   * @return RecordToInstance
   */
  private RecordToEntity mapRowToRecordToEntity(Row row, EntityTable entityTable) {
    return RecordToEntity.builder()
      .table(entityTable)
      .recordId(row.getValue(entityTable.getRecordIdFieldName()).toString())
      .entityId(row.getValue(entityTable.getEntityIdFieldName()).toString())
      .build();
  }

  /**
   * Prepares SQL query to Insert
   * @param entityTable the entity table
   * @return sql query to use
   */
  private String prepareQuery(EntityTable entityTable) {
    return INSERT_FUNCTION.replace("{recordIdFieldName}", entityTable.getRecordIdFieldName())
      .replace("{entityIdFieldName}", entityTable.getEntityIdFieldName())
      .replace("{tableName}", entityTable.getTableName());
  }
}
