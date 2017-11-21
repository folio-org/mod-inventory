package org.folio.inventory.support.http.server;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import org.folio.inventory.support.http.client.Response;

public class ForwardResponse {
  private ForwardResponse() { }

  public static void forward(HttpServerResponse forwardTo,
                             Response forwardFrom) {

    forwardTo.setStatusCode(forwardFrom.getStatusCode());

    if(forwardFrom.hasBody()) {
      Buffer buffer = Buffer.buffer(forwardFrom.getBody(), "UTF-8");

      forwardTo.putHeader(HttpHeaders.CONTENT_TYPE, forwardFrom.getContentType());
      forwardTo.putHeader(HttpHeaders.CONTENT_LENGTH, Integer.toString(buffer.length()));

      forwardTo.write(buffer);
      forwardTo.end();
    }
    else {
      forwardTo.end();
    }
  }

}
