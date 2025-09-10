package org.folio.inventory.support.http.server;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;

import static javax.ws.rs.core.HttpHeaders.LOCATION;
import static org.folio.inventory.client.util.ClientWrapperUtil.APPLICATION_JSON;
import static org.folio.inventory.client.util.ClientWrapperUtil.CONTENT_TYPE;

public final class RedirectResponse {

  private RedirectResponse() {
    throw new UnsupportedOperationException("Cannot instantiate utility class");
  }

  /**
   * Ends up response with CREATED(201) status, writes "Location" header,
   * and includes response body before ending.
   *
   * @param response http server response
   * @param location value to put to "Location" header
   * @param body     response body
   */
  public static void created(HttpServerResponse response, String location, JsonObject body) {
    locationResponse(response, location, body, HttpResponseStatus.CREATED.code());
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
   * Ends up response with ACCEPTED(202) status and writes "Location" header to the response body before ending.
   *
   * @param response http server response
   * @param location value to put to "Location" header
   */
  public static void accepted(HttpServerResponse response, String location) {
    locationResponse(response, location, HttpResponseStatus.ACCEPTED.code());
  }

  /**
   * Ends up response with INTERNAL_SERVER_ERROR(500) status and writes response body before ending.
   *
   * @param response http server response
   * @param body     response body
   */
  public static void serverError(HttpServerResponse response, Buffer body) {
    response.setStatusCode(HttpResponseStatus.INTERNAL_SERVER_ERROR.code());
    response.end(body);
  }

  private static void locationResponse(
    HttpServerResponse response,
    String url,
    int status) {

    response.headers().add("Location", url);
    response.setStatusCode(status);
    response.end();
  }

  private static void locationResponse(HttpServerResponse response, String url,
                                       JsonObject body, int status) {
    response.headers().set(LOCATION, url);
    response.headers().set(CONTENT_TYPE, APPLICATION_JSON);
    response.setStatusCode(status);
    response.end(Buffer.buffer(body.encodePrettily()));
  }
}
