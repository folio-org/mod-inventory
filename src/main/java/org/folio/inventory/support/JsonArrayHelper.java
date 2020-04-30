package org.folio.inventory.support;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class JsonArrayHelper {
  public static List<JsonObject> toList(JsonArray array) {
    if (array == null) {
      return Collections.emptyList();
    }

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
      .filter(Objects::nonNull)
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

  public static <T> List<T> toList(JsonArray array, Function<JsonObject, T> mapper) {
    return toList(array).stream()
      .map(mapper)
      .collect(Collectors.toList());
  }
}
