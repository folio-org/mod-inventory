package org.folio.inventory.support;

import io.vertx.core.json.JsonObject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Set;

public class JsonHelper {

  public String readFile(InputStream is) throws IOException {
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

  public JsonObject getJsonFileAsJsonObject(String filePath) throws IOException {
    InputStream is = this.getClass().getResourceAsStream(filePath);
    return new JsonObject(this.readFile(is));
  }

  public Object getValueFromSingleKeyEntry(JsonObject jo) {
    Set<String> fieldNames = jo.fieldNames();
    if (fieldNames.size() == 1) {
      String key = fieldNames.iterator().next();
      return jo.getString(key);
    }
    return null;
  }

  public JsonObject getJsonObjectFromSingleKeyEntry(JsonObject jo) {
    Object o = this.getValueFromSingleKeyEntry(jo);
    if (o instanceof JsonObject) {
      return (JsonObject) o;
    }
    return null;
  }
}
