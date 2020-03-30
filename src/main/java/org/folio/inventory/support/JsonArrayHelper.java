package org.folio.inventory.support;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
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

  public static List<String> toListOfStrings(JsonArray jsonArray) {
    List<String> list = new ArrayList<>();
    if (jsonArray != null) {
      for (int i=0; i<jsonArray.size(); i++){
        list.add(jsonArray.getString(i));
      }
    }
    return list;
  }

  public static List<Map> toListOfMaps(JsonArray array) {
    return JsonArrayHelper.toList(array).stream()
      .map(it -> it.getMap())
      .collect(Collectors.toList());
  }

  public static <T> List<T> toList(JsonArray array, Function<JsonObject, T> mapper) {
    return toList(array).stream()
      .map(mapper)
      .collect(Collectors.toList());
  }
}
