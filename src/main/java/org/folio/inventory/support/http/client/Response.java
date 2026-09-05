package org.folio.inventory.support.http.client;

import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonObject;

public record Response(String body, int statusCode, String contentType, String location) {
  public Response(int statusCode, String body, String contentType, String location) {
    this(body, statusCode, contentType, location);
  }

  public static Response from(HttpClientResponse response, Buffer body) {
    return new Response(response.statusCode(),
      BufferHelper.stringFromBuffer(body),
      convertNullToEmpty(response.getHeader(CONTENT_TYPE.toString())),
      response.getHeader("Location"));
  }

  public boolean hasBody() {
    return body() != null && !body().trim().isEmpty();
  }

  public JsonObject getJson() {
    String body = body();

    if (hasBody()) {
      return new JsonObject(body);
    } else {
      return new JsonObject();
    }
  }

  private static String convertNullToEmpty(String text) {
    return text != null ? text : "";
  }
}
