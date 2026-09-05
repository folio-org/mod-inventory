package org.folio.inventory.support.http.server;

import static io.vertx.core.http.HttpHeaders.CONTENT_LENGTH;
import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.support.http.ContentType;
import org.folio.inventory.support.http.client.Response;

public class ForwardResponse {
  private ForwardResponse() { }

  public static void forward(HttpServerResponse forwardTo, Response forwardFrom) {
    forward(forwardTo, forwardFrom.body(), forwardFrom.statusCode(),
      forwardFrom.contentType());
  }

  public static void forward(HttpServerResponse forwardTo, Failure forwardFrom) {
    forward(forwardTo, forwardFrom.reason(), forwardFrom.statusCode(),
      ContentType.TEXT_PLAIN);
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
