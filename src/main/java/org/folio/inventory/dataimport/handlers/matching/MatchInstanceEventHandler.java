package org.folio.inventory.dataimport.handlers.matching;

import org.folio.rest.jaxrs.model.EntityType;

import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_NOT_MATCHED;

public class MatchInstanceEventHandler extends AbstractMatchEventHandler {

  @Override
  EntityType getEntityType() {
    return EntityType.INSTANCE;
  }

  @Override
  String getMatchedEventType() {
    return DI_INVENTORY_INSTANCE_MATCHED.value();
  }

  @Override
  String getNotMatchedEventType() {
    return DI_INVENTORY_INSTANCE_NOT_MATCHED.value();
  }
}
