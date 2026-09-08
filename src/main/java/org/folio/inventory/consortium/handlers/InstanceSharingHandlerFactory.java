package org.folio.inventory.consortium.handlers;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import org.folio.inventory.consortium.util.InstanceOperationsHelper;
import org.folio.inventory.consortium.util.RestDataImportHelper;
import org.folio.inventory.consortium.util.SourceStorageHelper;
import org.folio.inventory.services.EntitiesLinksServiceImpl;
import org.folio.inventory.storage.Storage;

public enum InstanceSharingHandlerFactory {
  FOLIO, MARC;

  public static InstanceSharingHandler getInstanceSharingHandler(InstanceSharingHandlerFactory handlerType,
                                                                 InstanceOperationsHelper helper, Storage storage,
                                                                 Vertx vertx, HttpClient httpClient) {
    return handlerType == FOLIO
           ? new FolioInstanceSharingHandlerImpl(helper)
           : new MarcInstanceSharingHandlerImpl(helper, storage,
             new RestDataImportHelper(vertx), new EntitiesLinksServiceImpl(vertx, httpClient),
             new SourceStorageHelper(httpClient));
  }
}
