package org.folio.inventory.support.http.server;

import static io.vertx.core.http.HttpHeaders.CONTENT_LENGTH;
import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;

import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.support.http.client.Response;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;

public class ForwardResponse {
  private ForwardResponse() { }

  public static void forward(HttpServerResponse forwardTo, Response forwardFrom) {
    forward(forwardTo, forwardFrom.getBody(), forwardFrom.getStatusCode(),
      forwardFrom.getContentType());
  }

  public static void forward(HttpServerResponse forwardTo,
    String body, int statusCode, String contentType) {

    forwardTo.setStatusCode(statusCode);

    if (StringUtils.isNotBlank(body)) {
      Buffer buffer = Buffer.buffer(body, "UTF-8");

      forwardTo.putHeader(CONTENT_TYPE, contentType);
      forwardTo.putHeader(CONTENT_LENGTH, Integer.toString(buffer.length()));

      forwardTo.write(buffer);
    }

    forwardTo.end();
  }

}
