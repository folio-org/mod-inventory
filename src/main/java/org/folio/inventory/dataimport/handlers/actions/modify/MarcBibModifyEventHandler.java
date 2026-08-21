package org.folio.inventory.dataimport.handlers.actions.modify;

import io.vertx.core.http.HttpClient;
import org.folio.inventory.dataimport.cache.DeleteRuleFor999FieldCache;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.actions.InstanceUpdateDelegate;
import org.folio.inventory.dataimport.handlers.actions.PrecedingSucceedingTitlesHelper;
import org.folio.rest.jaxrs.model.EntityType;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;

public class MarcBibModifyEventHandler extends AbstractModifyEventHandler {

  public MarcBibModifyEventHandler(MappingMetadataCache mappingMetadataCache,
                                   DeleteRuleFor999FieldCache deleteRuleFor999FieldCache,
                                   InstanceUpdateDelegate instanceUpdateDelegate,
                                   PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper, HttpClient client) {
    super(mappingMetadataCache, deleteRuleFor999FieldCache, instanceUpdateDelegate, precedingSucceedingTitlesHelper, client);
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
