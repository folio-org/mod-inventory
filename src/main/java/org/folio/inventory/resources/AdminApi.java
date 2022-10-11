package org.folio.inventory.resources;

import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class AdminApi {
  private static final String HEALTH_PATH = "/admin/health";

  public void register(Router router) {
    router.get(HEALTH_PATH).handler(this::health);
  }

  public void health(RoutingContext routingContext) {
    routingContext.response().setStatusCode(200).end("OK");
  }
}
