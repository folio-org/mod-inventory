package org.folio.inventory.support.http.server;

import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import org.apache.commons.lang3.StringUtils;

public final class ServerErrorResponse {
  private ServerErrorResponse() { }

  public static void internalError(HttpServerResponse response, String reason) {
    response.setStatusCode(500);

    response.putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN.toString());
    response.end(reason);
  }

  public static void internalError(HttpServerResponse response, Throwable ex) {
    String message = StringUtils.isNotBlank(ex.getMessage())
                     ? ex.getMessage()
                     : "Unexpected exception occurred";

    internalError(response, message);
  }
}
