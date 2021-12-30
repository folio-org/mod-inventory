package org.folio.inventory.domain.relationship;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class RecordToEntity {
  private final EntityTable table;
  private final String recordId;
  private final String entityId;

  @Override
  public String toString() {
    return "RecordToEntity(table=" + this.getTable().getTableName() + ", recordId=" + this.getRecordId() + ", entityId=" + this.getEntityId() + ")";
  }
}
