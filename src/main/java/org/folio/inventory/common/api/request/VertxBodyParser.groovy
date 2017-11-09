package org.folio.inventory.common.api.request

import io.vertx.core.json.JsonObject
import io.vertx.ext.web.RoutingContext

class VertxBodyParser {
  Map toMap(RoutingContext routingContext) {
    println("Received Body: ${routingContext.bodyAsString}")

    if (hasBody(routingContext)) {
      routingContext.getBodyAsJson().map
    } else {
      new JsonObject().map
    }
  }

  private static boolean hasBody(RoutingContext routingContext) {
    routingContext.bodyAsString != null &&
      routingContext.bodyAsString.trim()
  }

}
