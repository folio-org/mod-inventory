package org.folio.inventory.instanceingress.handler;

import io.vertx.core.Future;
import io.vertx.core.http.HttpClient;
import java.util.concurrent.CompletableFuture;
import org.folio.DataImportEventPayload;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.handlers.actions.CreateInstanceEventHandler;
import org.folio.inventory.dataimport.handlers.actions.PrecedingSucceedingTitlesHelper;
import org.folio.inventory.services.IdStorageService;
import org.folio.inventory.storage.Storage;

public class CreateInstanceIngressEventHandler extends CreateInstanceEventHandler {

  public CreateInstanceIngressEventHandler(Storage storage,
                                           PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper,
                                           MappingMetadataCache mappingMetadataCache,
                                           IdStorageService idStorageService,
                                           HttpClient httpClient) {
    super(storage,precedingSucceedingTitlesHelper, mappingMetadataCache, idStorageService,
      (eventPayload, targetEventType, context) -> Future.succeededFuture(), httpClient);
  }

  @Override
  public boolean isEligible(DataImportEventPayload dataImportEventPayload) {
    return false;
  }

  @Override
  public String getPostProcessingInitializationEventType() {
    return "";
  }

}
