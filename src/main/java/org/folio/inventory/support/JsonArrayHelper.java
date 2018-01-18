package org.folio.inventory.support;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JsonArrayHelper {
  private static Stream<Object> stream(JsonArray array) {
    if (array == null) {
      return Stream.empty();
    }
    return array.stream();
  }

  /**
   * Collect the JsonObject elements of array into a modifiable List, ignore
   * non JsonObject elements.
   * @param array  where to search; may be null for returning an empty list
   * @return the modifiable List
   */
  public static List<JsonObject> toList(JsonArray array) {
    return stream(array)
      .filter(it -> it instanceof JsonObject)
      .map(it -> (JsonObject)it)
      .collect(Collectors.toCollection(ArrayList::new));
  }

  /**
   * Collect the CharSequence elements of jsonArray into a modifiable List,
   * any element may be null.
   * Throw an exception if any element is not a CharSequence.
   *
   * @param jsonArray  where to seearch; may be null for returning an empty list
   * @return the modifiable list
   * @throws java.lang.ClassCastException if the value cannot be converted to String
   */
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
}
