package org.folio.inventory.common.api.response

import io.vertx.core.http.HttpServerResponse
import org.apache.http.entity.ContentType
import org.folio.inventory.common.domain.Failure

import java.util.function.Consumer

class FailureResponseConsumer {
  static Consumer<Failure> serverError(HttpServerResponse response) {
    return { Failure failure ->
      switch (failure.statusCode) {
        case 300..599:
          response.setStatusCode(failure.statusCode)
          response.putHeader "content-type", ContentType.TEXT_PLAIN.toString()
          response.end("${failure.reason}")
          break

        default:
          ServerErrorResponse.internalError(response, failure.reason)
      }
    }
  }
}
