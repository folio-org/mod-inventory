package org.folio.inventory.domain.relationship;

import lombok.Getter;

/**
 * Enum which stores pointer on DB-tableNames inside.
 */
@Getter
public enum EventTable {
  SHARED_INSTANCE("events_shared_instances");

  private final String tableName;

  EventTable(String tableName) {
    this.tableName = tableName;
  }
}
