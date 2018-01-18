package org.folio.inventory.common;

import io.vertx.core.logging.Logger;
import io.vertx.ext.web.RoutingContext;

/**
 * Log details of a io.vertx.ext.web.RoutingContext.
 */
public class WebRequestDiagnostics {
  private WebRequestDiagnostics() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  /**
   * Write method, path and headers of routingContext to log.
   * @param log  where to log
   * @param routingContext  the context
   */
  public static void outputDiagnostics(Logger log, RoutingContext routingContext) {

    log.debug(String.format("Handling %s %s", routingContext.request().rawMethod(),
      routingContext.normalisedPath()));

    outputHeaders(log, routingContext);

    routingContext.next();
  }

  private static void outputHeaders(Logger log, RoutingContext routingContext) {
    log.debug("Headers");

    for (String name : routingContext.request().headers().names()) {
      for (String entry : routingContext.request().headers().getAll(name))
        log.debug(String.format("%s : %s", name, entry));
    }
  }
}
