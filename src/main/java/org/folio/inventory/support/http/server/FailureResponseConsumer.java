package org.folio.inventory.support.http.server;

import io.vertx.core.http.HttpServerResponse;
import org.apache.http.entity.ContentType;
import org.folio.inventory.common.domain.Failure;

import java.util.function.Consumer;

public class FailureResponseConsumer {
  public static Consumer<Failure> serverError(final HttpServerResponse response) {

    return failure -> {
      if (failure.getStatusCode() >= 300 && failure.getStatusCode() <= 599) {
        response.setStatusCode(failure.getStatusCode());
        response.putHeader("content-type", ContentType.TEXT_PLAIN.toString());
        response.end(String.format(failure.getReason()));
      } else {
        ServerErrorResponse.internalError(response, failure.getReason());
      }
    };
  }
}
