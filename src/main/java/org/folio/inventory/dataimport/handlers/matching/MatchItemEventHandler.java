package org.folio.inventory.dataimport.handlers.matching;

import org.folio.rest.jaxrs.model.EntityType;

public class MatchItemEventHandler extends AbstractMatchEventHandler {

  @Override
  EntityType getEntityType() {
    return EntityType.ITEM;
  }

  @Override
  String getMatchedEventType() {
    return "DI_INVENTORY_ITEM_MATCHED";
  }

  @Override
  String getNotMatchedEventType() {
    return "DI_INVENTORY_ITEM_NOT_MATCHED";
  }
}
