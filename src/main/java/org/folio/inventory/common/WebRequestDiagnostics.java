package org.folio.inventory.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.vertx.ext.web.RoutingContext;

import java.lang.invoke.MethodHandles;

public class WebRequestDiagnostics {
  private WebRequestDiagnostics() {

  }

  private static final Logger log = LogManager.getLogger(MethodHandles.lookup().lookupClass());

  public static void outputDiagnostics(RoutingContext routingContext) {

    log.info("Handling {} {}", routingContext.request().method().name(), routingContext.normalizedPath());

    outputHeaders(routingContext);

    routingContext.next();
  }

  private static void outputHeaders(RoutingContext routingContext) {
    log.debug("Headers");

    for (String name : routingContext.request().headers().names()) {
      for (String entry : routingContext.request().headers().getAll(name))
        log.debug(String.format("%s : %s", name, entry));
    }
  }
}
