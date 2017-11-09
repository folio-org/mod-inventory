package org.folio.inventory.common.api.response

import io.vertx.core.http.HttpServerResponse

class RedirectResponse {
  static redirect(HttpServerResponse response, String url) {
    locationResponse(response, url, 303)
  }


  static created(HttpServerResponse response, String url) {
    locationResponse(response, url, 201)
  }

  static accepted(HttpServerResponse response, String url) {
    locationResponse(response, url, 202)
  }

  private static void locationResponse(response, String url, Integer status) {
    response.headers().add("Location", url)
    response.setStatusCode(status)
    response.end()
  }
}
