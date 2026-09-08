package org.folio.inventory.support;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.NonNull;

public final class JsonArrayHelper {
  private JsonArrayHelper() { }

  public static List<JsonObject> toList(@NonNull JsonObject json, String propertyName) {
    return toList(json.getJsonArray(propertyName));
  }

  public static List<JsonObject> toList(JsonArray array) {
    if (array == null) {
      return Collections.emptyList();
    }

    return array
      .stream()
      .map(it -> {
        if (it instanceof JsonObject jsonObject) {
          return jsonObject;
        } else {
          return null;
        }
      })
      .filter(Objects::nonNull)
      .collect(Collectors.toList());
  }

  public static <T> List<T> toList(JsonArray array, Function<JsonObject, T> mapper) {
    return toList(array).stream()
      .map(mapper)
      .collect(Collectors.toList());
  }

  public static List<String> toListOfStrings(@NonNull JsonObject json, String propertyName) {
    return toListOfStrings(json.getJsonArray(propertyName));
  }

  public static List<String> toListOfStrings(JsonArray array) {
    if (array == null) {
      return Collections.emptyList();
    }

    return IntStream.range(0, array.size())
      .mapToObj(array::getString)
      .collect(Collectors.toList());
  }
}
