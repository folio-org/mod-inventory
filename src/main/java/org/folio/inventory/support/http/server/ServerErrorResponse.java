package org.folio.inventory.support.http.server;

import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.support.http.ContentType;

import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;

public class ServerErrorResponse {
  private ServerErrorResponse() { }

  public static void internalError(HttpServerResponse response, String reason) {
    response.setStatusCode(500);

    response.putHeader(HttpHeaders.CONTENT_TYPE, ContentType.TEXT_PLAIN);
    response.end(reason);
  }

  public static void internalError(HttpServerResponse response, Throwable ex) {
    String message = StringUtils.isNotBlank(ex.getMessage())
      ? ex.getMessage()
      : "Unexpected exception occurred";

    internalError(response, message);
  }
}
