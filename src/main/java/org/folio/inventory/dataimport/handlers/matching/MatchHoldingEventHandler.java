package org.folio.inventory.dataimport.handlers.matching;

import org.folio.rest.jaxrs.model.EntityType;

public class MatchHoldingEventHandler extends AbstractMatchEventHandler {

  @Override
  EntityType getEntityType() {
    return EntityType.HOLDINGS;
  }

  @Override
  String getMatchedEventType() {
    return "DI_INVENTORY_HOLDING_MATCHED";
  }

  @Override
  String getNotMatchedEventType() {
    return "DI_INVENTORY_HOLDING_NOT_MATCHED";
  }
}
