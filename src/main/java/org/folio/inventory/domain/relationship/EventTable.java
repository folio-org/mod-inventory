package org.folio.inventory.domain.relationship;

public enum EventTable {
  SHARED_INSTANCE("events_shared_instances");

  private final String tableName;

  EventTable(String tableName) {
    this.tableName = tableName;
  }

  public String getTableName() {
    return tableName;
  }
}
