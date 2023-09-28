package org.folio.inventory.domain.relationship;

import lombok.Builder;
import lombok.Getter;

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
