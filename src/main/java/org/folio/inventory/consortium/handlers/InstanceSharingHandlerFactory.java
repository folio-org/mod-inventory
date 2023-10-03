package org.folio.inventory.consortium.handlers;

import io.vertx.core.Vertx;
import org.folio.inventory.consortium.util.InstanceOperationsHelper;

public enum InstanceSharingHandlerFactory {
  FOLIO, MARC;

  public static InstanceSharingHandler getInstanceSharingHandler(InstanceSharingHandlerFactory instanceSharingHandlerType,
                                                                 InstanceOperationsHelper instanceOperationsHelper, Vertx vertx) {
    return instanceSharingHandlerType == FOLIO ?
      new FolioInstanceSharingHandlerImpl(instanceOperationsHelper) :
      new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, vertx);
  }

}
