package org.folio.inventory.resources;

import io.vertx.core.http.HttpClient;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.folio.inventory.storage.Storage;

public class UpdateOwnershipApi extends AbstractInventoryResource {
  public UpdateOwnershipApi(Storage storage, HttpClient client) {
    super(storage, client);
  }

  @Override
  public void register(Router router) {
    router.post("/inventory/items/update-ownership")
      .handler(this::updateItemsOwnership);
    router.post("/inventory/holdings/update-ownership")
      .handler(this::updateHoldingsOwnership);
  }

  private void updateHoldingsOwnership(RoutingContext routingContext) {

  }

  private void updateItemsOwnership(RoutingContext routingContext) {

  }
}
