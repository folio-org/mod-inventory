package org.folio.inventory.dataimport.handlers.matching;

import io.vertx.core.http.HttpClient;
import org.folio.inventory.consortium.services.ConsortiumService;
import org.folio.rest.jaxrs.model.RecordMatchingDto;

import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MATCHED;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MATCHED_READY_FOR_POST_PROCESSING;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_NOT_MATCHED;

public class MarcBibliographicMatchEventHandler extends AbstractMarcMatchEventHandler {

  public MarcBibliographicMatchEventHandler(ConsortiumService consortiumService, HttpClient httpClient) {
    super(consortiumService, DI_SRS_MARC_BIB_RECORD_MATCHED, DI_SRS_MARC_BIB_RECORD_NOT_MATCHED, httpClient);
  }

  @Override
  protected RecordMatchingDto.RecordType getMatchedRecordType() {
    return RecordMatchingDto.RecordType.MARC_BIB;
  }

  @Override
  protected String getMarcType() {
    return MARC_BIBLIOGRAPHIC.value();
  }

  @Override
  public boolean isPostProcessingNeeded() {
    return true;
  }

  @Override
  public String getPostProcessingInitializationEventType() {
    return DI_SRS_MARC_BIB_RECORD_MATCHED_READY_FOR_POST_PROCESSING.value();
  }

  @Override
  protected boolean isMatchingOnCentralTenantRequired() {
    return true;
  }

}
