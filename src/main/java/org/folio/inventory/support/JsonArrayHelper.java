package org.folio.inventory.support;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JsonArrayHelper {
  public static List<JsonObject> toList(JsonArray array) {
    return array
      .stream()
      .map(it -> {
        if(it instanceof JsonObject) {
          return (JsonObject)it;
        }
        else {
          return null;
        }
      })
      .filter(it -> it != null)
      .collect(Collectors.toList());
  }

  public static List<Map> toListOfMaps(JsonArray array) {
    return JsonArrayHelper.toList(array).stream()
      .map(it -> it.getMap())
      .collect(Collectors.toList());
  }
}
