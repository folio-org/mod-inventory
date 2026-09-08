package org.folio.inventory.support.http.server;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public record ValidationError(String message, String propertyName, String value) {

  public JsonObject toJson() {
    JsonArray parameters = new JsonArray();

    parameters.add(new JsonObject()
      .put("key", propertyName)
      .put("value", value));

    return new JsonObject()
      .put("message", message)
      .put("parameters", parameters);
  }
}
