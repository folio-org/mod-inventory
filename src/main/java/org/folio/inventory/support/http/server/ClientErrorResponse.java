package org.folio.inventory.support.http.server;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;

public final class ClientErrorResponse {
  private ClientErrorResponse() { }

  public static void notFound(HttpServerResponse response) {
    notFound(response, "Not Found");
  }

  public static void notFound(HttpServerResponse response, String message) {
    response.setStatusCode(404);
    response.end(message);
  }

  public static void badRequest(HttpServerResponse response, String reason) {
    response.setStatusCode(400);
    response.putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN.toString());
    response.end(reason);
  }

  public static void forbidden(HttpServerResponse response) {
    response.setStatusCode(403);
    response.end();
  }

  public static void optimisticLocking(HttpServerResponse response, String reason) {
    response.setStatusCode(409);
    response.putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN.toString());
    response.end(reason);
  }
}
