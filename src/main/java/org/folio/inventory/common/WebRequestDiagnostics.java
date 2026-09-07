package org.folio.inventory.common;

import io.vertx.ext.web.RoutingContext;
import java.lang.invoke.MethodHandles;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class WebRequestDiagnostics {

  private static final Logger log = LogManager.getLogger(MethodHandles.lookup().lookupClass());

  private WebRequestDiagnostics() { }

  public static void outputDiagnostics(RoutingContext routingContext) {
    var httpMethod = routingContext.request().method().name();
    var requestPath = routingContext.normalizedPath();
    log.info("Handling {} {}", httpMethod, requestPath);

    outputHeaders(routingContext);

    routingContext.next();
  }

  private static void outputHeaders(RoutingContext routingContext) {
    log.debug("Headers");

    for (String name : routingContext.request().headers().names()) {
      for (String entry : routingContext.request().headers().getAll(name)) {
        log.debug("{} : {}", name, entry);
      }
    }
  }
}
