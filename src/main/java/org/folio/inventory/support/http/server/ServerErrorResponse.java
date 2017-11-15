package org.folio.inventory.support.http.server;

import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import org.folio.inventory.support.http.ContentType;

public class ServerErrorResponse {
  private ServerErrorResponse() { }

  public static void internalError(HttpServerResponse response, String reason) {
    response.setStatusCode(500);

    response.putHeader(HttpHeaders.CONTENT_TYPE, ContentType.TEXT_PLAIN);
    response.end(reason);
  }
}
