package org.folio.inventory.support;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import io.vertx.core.json.JsonObject;

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
}
