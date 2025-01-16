package org.folio.inventory.support.http.server;

import java.util.Collections;
import java.util.List;

import org.folio.inventory.support.http.ContentType;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class JsonResponse {

  public static final String ERRORS = "errors";

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
    String message,
    String propertyName,
    String value) {

    ValidationError error = new ValidationError(message, propertyName, value);

    JsonArray errors = new JsonArray();

    errors.add(error.toJson());

    response(response, new JsonObject().put(ERRORS, errors), 422);
  }

  public static void unprocessableEntity(
    HttpServerResponse response,
    List<ValidationError> errors) {

    JsonArray errorsArray = new JsonArray();

    errors.forEach(error -> errorsArray.add(error.toJson()));

    response(response, new JsonObject().put(ERRORS, errorsArray), 422);
  }

  public static void unprocessableEntity(
    HttpServerResponse response, ValidationError error) {

    unprocessableEntity(response, Collections.singletonList(error));
  }

  public static void unprocessableEntity(HttpServerResponse response, String errorMessage) {
    JsonArray error = new JsonArray();
    error.add(errorMessage);
    response(response, new JsonObject().put("errors", error), 422);
  }

  public static void unprocessableEntity(HttpServerResponse response, JsonObject errors) {
    response(response, new JsonObject().put("errors", errors), 422);
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
