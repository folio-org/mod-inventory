package org.folio.inventory.support.http.server;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.stream.Collectors;

public class JsonResponse {

  //TODO: Needs a location
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
    response.putHeader("content-type", "application/json; charset=utf-8");
    response.putHeader("content-length", Integer.toString(buffer.length()));

    response.write(buffer);
    response.end();
  }
}
