package org.folio.inventory.support.http.server;

import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import org.folio.inventory.support.http.ContentType;

public class ClientErrorResponse {
  private ClientErrorResponse() { }

  public static void notFound(HttpServerResponse response) {
    response.setStatusCode(404);
    response.end("Not Found");
  }

  public static void badRequest(HttpServerResponse response, String reason) {
    response.setStatusCode(400);
    response.putHeader(HttpHeaders.CONTENT_TYPE, ContentType.TEXT_PLAIN);
    response.end(reason);
  }

  public static void forbidden(HttpServerResponse response) {
    response.setStatusCode(403);
    response.end();
  }
}
