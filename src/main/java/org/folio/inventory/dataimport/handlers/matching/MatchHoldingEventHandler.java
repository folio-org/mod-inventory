package org.folio.inventory.dataimport.handlers.matching;

import org.folio.rest.jaxrs.model.EntityType;

import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_NOT_MATCHED;

public class MatchHoldingEventHandler extends AbstractMatchEventHandler {

  @Override
  protected EntityType getEntityType() {
    return EntityType.HOLDINGS;
  }

  @Override
  protected String getMatchedEventType() {
    return DI_INVENTORY_HOLDING_MATCHED.value();
  }

  @Override
  protected String getNotMatchedEventType() {
    return DI_INVENTORY_HOLDING_NOT_MATCHED.value();
  }
}
