package org.folio.inventory.dataimport.handlers.matching;

import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_HOLDING_NOT_MATCHED;

import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.rest.jaxrs.model.EntityType;

public class MatchHoldingEventHandler extends AbstractMatchEventHandler {

  public MatchHoldingEventHandler(MappingMetadataCache mappingMetadataCache, ConsortiumService consortiumService) {
    super(mappingMetadataCache, consortiumService);
  }

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

  @Override
  protected boolean isConsortiumActionAvailable() {
    return false;
  }
}
