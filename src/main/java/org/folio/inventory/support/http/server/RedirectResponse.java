package org.folio.inventory.support.http.server;

import io.vertx.core.http.HttpServerResponse;

public class RedirectResponse {
  public static void redirect(HttpServerResponse response, String url) {
    locationResponse(response, url, 303);
  }

  public static void created(HttpServerResponse response, String url) {
    locationResponse(response, url, 201);
  }

  public static void accepted(HttpServerResponse response, String url) {
    locationResponse(response, url, 202);
  }

  private static void locationResponse(
    HttpServerResponse response,
    String url,
    Integer status) {

    response.headers().add("Location", url);
    response.setStatusCode(status);
    response.end();
  }

}
