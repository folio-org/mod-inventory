package org.folio.inventory.support;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class JsonHelper {
  public static String getNestedProperty(
    JsonObject representation,
    String objectPropertyName,
    String nestedPropertyName) {

    return representation.containsKey(objectPropertyName)
      ? representation.getJsonObject(objectPropertyName).getString(nestedPropertyName)
      : null;
  }

  public static String getString(JsonObject representation, String propertyName) {
    if (representation != null) {
      return representation.getString(propertyName);
    }

    return null;
  }

  public static <T> void includeIfPresent(
    JsonObject representation, String propertyName, T value) {

    if (representation != null && isNotBlank(propertyName) && value != null) {
      representation.put(propertyName, value);
    }
  }

  public JsonObject getJsonFileAsJsonObject(String filePath) throws IOException {
    InputStream is = this.getClass().getResourceAsStream(filePath);
    return new JsonObject(readFile(is));
  }

  /**
   * Update JsonObject representation with the given property name and only not null values.
   *
   * @param representation The JsonObject representation to update.
   * @param propertyName   The name of the property to set.
   * @param obj            The value to set the property with.
   */
  public static void putNotNullValues(JsonObject representation, String propertyName, Object obj) {
    if (obj != null && isNotBlank(propertyName)) {
      if (obj instanceof Collection<?> collection) {
        representation.put(propertyName, new JsonArray(toNotNullList(collection)));
      } else if (obj instanceof JsonArray array) {
        representation.put(propertyName, toNotNullList(array.getList()));
      } else {
        JsonObject json = JsonObject.mapFrom(obj);
        handleNullNestedFields(json);
        representation.put(propertyName, json);
      }
    }
  }

  private String readFile(InputStream is) throws IOException {
    try (BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
      StringBuilder sb = new StringBuilder();
      String line = br.readLine();
      while (line != null) {
        sb.append(line);
        sb.append("\n");
        line = br.readLine();
      }
      return sb.toString();
    }
  }

  private static JsonObject handleNullNestedFields(JsonObject itemObject) {
    var keysToRemove = new ArrayList<String>();
    for (var key : itemObject.fieldNames()) {
      var value = itemObject.getValue(key);
      if (value == null) {
        keysToRemove.add(key);
      } else if (value instanceof JsonObject object) {
        handleNullNestedFields(object);
      }
    }

    keysToRemove.forEach(itemObject::remove);
    return itemObject;
  }

  private static List<Iterable<?>> toNotNullList(Collection<?> collection) {
    return collection.stream()
      .filter(Objects::nonNull)
      .map(item -> {
        if (item instanceof Collection<?> iterable) {
          return toNotNullList(iterable);
        } else if (item instanceof JsonArray array) {
          return toNotNullList(array.getList());
        } else {
          var jsonItem = JsonObject.mapFrom(item);
          handleNullNestedFields(jsonItem);
          return jsonItem;
        }
      })
      .toList();
  }
}
