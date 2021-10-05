package org.folio.inventory.resources;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.folio.inventory.config.InventoryConfiguration;
import org.folio.inventory.config.InventoryConfigurationImpl;
import org.folio.inventory.support.http.server.JsonResponse;

import java.util.Set;

public class InventoryConfigApi {
  private static final String INSTANCE_BLOCKED_FIELDS_CONFIG_PATH = "/inventory/config/instances/blocked-fields";
  private static final String HOLDINGS_BLOCKED_FIELDS_CONFIG_PATH = "/inventory/config/holdings/blocked-fields";
  protected final InventoryConfiguration config = new InventoryConfigurationImpl();

  public void register(Router router) {
    router.get(INSTANCE_BLOCKED_FIELDS_CONFIG_PATH).handler(routingContext -> getBlockedFields(routingContext, config.getInstanceBlockedFields()));
    router.get(HOLDINGS_BLOCKED_FIELDS_CONFIG_PATH).handler(routingContext -> getBlockedFields(routingContext, config.getHoldingsBlockedFields()));
  }

  private void getBlockedFields(RoutingContext routingContext, Set<String> blockedFields) {
    JsonObject response = new JsonObject();
    response.put("blockedFields", new JsonArray(Json.encode(blockedFields)));
    JsonResponse.success(routingContext.response(), response);
  }
}
