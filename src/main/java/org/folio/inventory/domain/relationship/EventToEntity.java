package org.folio.inventory.domain.relationship;

import lombok.Builder;
import lombok.Getter;

/**
 * Relationship between eventId and DB-table information.
 */
@Builder
@Getter
public class EventToEntity {

  private final EventTable table;
  private final String eventId;

  @Override
  public String toString() {
    return "EventToEntity{" +
      "table=" + table +
      ", eventId='" + eventId + '\'' +
      '}';
  }
}
