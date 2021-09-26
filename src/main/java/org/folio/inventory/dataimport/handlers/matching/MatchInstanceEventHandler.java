package org.folio.inventory.dataimport.handlers.matching;

import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.rest.jaxrs.model.EntityType;

import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_NOT_MATCHED;

public class MatchInstanceEventHandler extends AbstractMatchEventHandler {

  public MatchInstanceEventHandler(MappingMetadataCache mappingMetadataCache) {
    super(mappingMetadataCache);
  }

  @Override
  protected EntityType getEntityType() {
    return EntityType.INSTANCE;
  }

  @Override
  protected String getMatchedEventType() {
    return DI_INVENTORY_INSTANCE_MATCHED.value();
  }

  @Override
  protected String getNotMatchedEventType() {
    return DI_INVENTORY_INSTANCE_NOT_MATCHED.value();
  }
}
