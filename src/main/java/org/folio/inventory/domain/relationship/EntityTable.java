package org.folio.inventory.domain.relationship;

public enum EntityTable {
  INSTANCE("records_instances", "record_id", "instance_id");

  private final String tableName;
  private final String recordIdFieldName;
  private final String entityIdFieldName;

  EntityTable(String tableName, String recordIdFieldName, String entityIdFieldName) {
    this.tableName = tableName;
    this.recordIdFieldName = recordIdFieldName;
    this.entityIdFieldName = entityIdFieldName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getRecordIdFieldName() {
    return recordIdFieldName;
  }

  public String getEntityIdFieldName() {
    return entityIdFieldName;
  }
}
