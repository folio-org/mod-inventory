package org.folio.inventory.dataimport.handlers.matching;

import org.folio.rest.jaxrs.model.EntityType;

public class MatchItemEventHandler extends AbstractMatchEventHandler {

  @Override
  protected EntityType getEntityType() {
    return EntityType.ITEM;
  }

  @Override
  protected String getMatchedEventType() {
    return "DI_INVENTORY_ITEM_MATCHED";
  }

  @Override
  protected String getNotMatchedEventType() {
    return "DI_INVENTORY_ITEM_NOT_MATCHED";
  }
}
