package org.folio.inventory.dataimport.handlers.actions.modify;

import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.dataimport.handlers.actions.PrecedingSucceedingTitlesHelper;
import org.folio.rest.jaxrs.model.EntityType;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;

public class MarcBibModifyEventHandler extends AbstractModifyEventHandler {

  protected MarcBibModifyEventHandler(MappingMetadataCache mappingMetadataCache,
                                      InstanceUpdateDelegate instanceUpdateDelegate,
                                      PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper) {
    super(mappingMetadataCache, instanceUpdateDelegate, precedingSucceedingTitlesHelper);
  }

  @Override
  protected EntityType modifiedEntityType() {
    return MARC_BIBLIOGRAPHIC;
  }

  @Override
  protected EntityType relatedEntityType() {
    return INSTANCE;
  }

  @Override
  public boolean isPostProcessingNeeded() {
    return false;
  }

  @Override
  protected String modifyEventType() {
    return DI_SRS_MARC_BIB_RECORD_MODIFIED.value();
  }
}
