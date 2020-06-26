package api.support.builders;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.UUID;

public abstract class AbstractBuilder implements Builder {

  void includeWhenPresent(
    JsonObject itemRequest,
    String property,
    UUID value) {

    if(value != null) {
      itemRequest.put(property, value.toString());
    }
  }

  void includeWhenPresent(
    JsonObject itemRequest,
    String property,
    String value) {

    if(value != null) {
      itemRequest.put(property, value);
    }
  }

  void includeWhenPresent(
    JsonObject itemRequest,
    String property,
    JsonObject value) {

    if(value != null) {
      itemRequest.put(property, value);
    }
  }

  void includeWhenPresent(
    JsonObject itemRequest,
    String property,
    JsonArray value
  ) {
    if (value != null) {
      itemRequest.put(property, value);
    }
  }
}
