package org.folio.inventory.support.http.server;

import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import java.util.function.Consumer;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.support.http.ContentType;

public class FailureResponseConsumer {
  private FailureResponseConsumer() { }

  public static Consumer<Failure> serverError(final HttpServerResponse response) {
    return failure -> {
      if (failure.statusCode() >= 300 && failure.statusCode() <= 599) {
        response.setStatusCode(failure.statusCode());
        response.putHeader(HttpHeaders.CONTENT_TYPE, ContentType.TEXT_PLAIN);
        response.end(failure.reason() == null ? "" : failure.reason());
      } else {
        ServerErrorResponse.internalError(response, failure.reason());
      }
    };
  }
}
