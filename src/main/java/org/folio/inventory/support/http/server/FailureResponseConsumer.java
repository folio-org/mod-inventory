package org.folio.inventory.support.http.server;

import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.support.http.ContentType;

import java.util.function.Consumer;

public class FailureResponseConsumer {
  private FailureResponseConsumer() { }

  public static Consumer<Failure> serverError(final HttpServerResponse response) {
    return failure -> {
      if (failure.getStatusCode() >= 300 && failure.getStatusCode() <= 599) {
        response.setStatusCode(failure.getStatusCode());
        response.putHeader(HttpHeaders.CONTENT_TYPE, ContentType.TEXT_PLAIN);
        response.end(failure.getReason() == null ? "" : failure.getReason());
      } else {
        ServerErrorResponse.internalError(response, failure.getReason());
      }
    };
  }
}
