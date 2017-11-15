package org.folio.inventory.support.http.server;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventory.support.http.ContentType;

import java.util.List;
import java.util.stream.Collectors;

public class JsonResponse {
  private JsonResponse() { }

  public static void created(HttpServerResponse response,
                      JsonObject body) {

    response(response, body, 201);
  }

  public static void success(HttpServerResponse response,
                             JsonObject body) {

    response(response, body, 200);
  }

  public static void unprocessableEntity(
    HttpServerResponse response,
    String message, List<ValidationError> errors) {

    JsonArray parameters = new JsonArray(errors.stream()
      .map(error -> new JsonObject()
        .put("key", error.propertyName)
        .put("value", error.value))
      .collect(Collectors.toList()));

    JsonObject wrappedErrors = new JsonObject()
      .put("message", message)
      .put("parameters", parameters);

    response(response, wrappedErrors, 422);
  }

  private static void response(HttpServerResponse response,
                               JsonObject body,
                               int statusCode) {

    String json = Json.encodePrettily(body);
    Buffer buffer = Buffer.buffer(json, "UTF-8");

    response.setStatusCode(statusCode);
    response.putHeader(HttpHeaders.CONTENT_TYPE, String.format("%s; charset=utf-8",
      ContentType.APPLICATION_JSON));

    response.putHeader(HttpHeaders.CONTENT_LENGTH, Integer.toString(buffer.length()));

    response.write(buffer);
    response.end();
  }
}
