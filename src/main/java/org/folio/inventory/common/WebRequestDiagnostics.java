package org.folio.inventory.common;

import io.vertx.ext.web.RoutingContext;

public class WebRequestDiagnostics {
  public static void outputDiagnostics(RoutingContext routingContext) {

    System.out.println(String.format("Handling %s\n", routingContext.normalisedPath()));
    System.out.println(String.format("Method: %s\n", routingContext.request().rawMethod()));

    outputHeaders(routingContext);

    routingContext.next();
  }

  private static void outputHeaders(RoutingContext routingContext) {
    System.out.println("Headers");

    for (String name : routingContext.request().headers().names()) {
      for (String entry : routingContext.request().headers().getAll(name))
        System.out.println(String.format("%s : %s\n", name, entry));
    }


    System.out.println();
  }
}
