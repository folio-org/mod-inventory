package org.folio.inventory.support.http.server;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;

public class RedirectResponse {

  /**
   * Ends up response with CREATED(201) status and writes "Location" header to the response body before ending.
   *
   * @param response http server response
   * @param location value to put to "Location" header
   */
  public static void created(HttpServerResponse response, String location) {
    locationResponse(response, location, HttpResponseStatus.CREATED.code());
  }

  /**
   * Ends up response with CREATED(201) status and writes data to the response body before ending.
   *
   * @param response http server response
   * @param body     response body
   */
  public static void created(HttpServerResponse response, Buffer body) {
    response.setStatusCode(HttpResponseStatus.CREATED.code());
    response.end(body);
  }

  /**
   * Ends up response with CREATED(202) status and writes "Location" header to the response body before ending.
   *
   * @param response http server response
   * @param location value to put to "Location" header
   */
  public static void accepted(HttpServerResponse response, String location) {
    locationResponse(response, location, HttpResponseStatus.ACCEPTED.code());
  }

  private static void locationResponse(
    HttpServerResponse response,
    String url,
    Integer status) {

    response.headers().add("Location", url);
    response.setStatusCode(status);
    response.end();
  }

}
