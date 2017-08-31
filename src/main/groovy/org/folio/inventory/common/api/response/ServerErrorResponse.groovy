package org.folio.inventory.common.api.response

import io.vertx.core.http.HttpServerResponse
import org.apache.http.entity.ContentType

class ServerErrorResponse {
  static internalError(HttpServerResponse response, String reason) {
    response.setStatusCode(500)

    response.putHeader "content-type", ContentType.TEXT_PLAIN.toString()
    response.end(reason)
  }
}
