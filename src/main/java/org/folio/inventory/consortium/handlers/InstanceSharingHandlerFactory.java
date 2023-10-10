package org.folio.inventory.consortium.handlers;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import org.folio.inventory.consortium.util.InstanceOperationsHelper;
import org.folio.inventory.storage.Storage;

public enum InstanceSharingHandlerFactory {
  FOLIO, MARC;

  public static InstanceSharingHandler getInstanceSharingHandler(InstanceSharingHandlerFactory instanceSharingHandlerType,
                                                                 InstanceOperationsHelper instanceOperationsHelper, Storage storage,
                                                                 Vertx vertx, HttpClient httpClient) {
    return instanceSharingHandlerType == FOLIO ?
      new FolioInstanceSharingHandlerImpl(instanceOperationsHelper) :
      new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, storage, vertx, httpClient);
  }

}
