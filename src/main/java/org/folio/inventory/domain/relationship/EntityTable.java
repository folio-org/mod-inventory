package org.folio.inventory.domain.relationship;

public enum EntityTable {
  INSTANCE("records_instances", "instance_id"),
  HOLDINGS("records_holdings", "holdings_id"),
  ITEM("records_items", "item_id");

  private static final String RECORD_ID_FIELD_NAME = "record_id";

  private final String tableName;
  private final String entityIdFieldName;

  EntityTable(String tableName, String entityIdFieldName) {
    this.tableName = tableName;
    this.entityIdFieldName = entityIdFieldName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getRecordIdFieldName() {
    return RECORD_ID_FIELD_NAME;
  }

  public String getEntityIdFieldName() {
    return entityIdFieldName;
  }
}
