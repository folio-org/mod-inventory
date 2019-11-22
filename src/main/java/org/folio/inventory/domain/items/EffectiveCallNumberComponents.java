package org.folio.inventory.domain.items;

import io.vertx.core.json.JsonObject;

public class EffectiveCallNumberComponents {

    private String callNumber;
    private String prefix;
    private String suffix;

    public EffectiveCallNumberComponents(String callNumber, String prefix, String suffix) {
      this.callNumber = callNumber;
      this.prefix = prefix;
      this.suffix = suffix;
    }

    public static EffectiveCallNumberComponents from(JsonObject representation) {
      if (representation == null) {
        return null;
      }

      return new EffectiveCallNumberComponents(
        representation.getString("callNumber"),
        representation.getString("prefix"),
        representation.getString("suffix"));
    }

    public JsonObject toJson() {
      JsonObject components = new JsonObject();
      components.put("callNumber", callNumber);
      components.put("prefix", prefix);
      components.put("suffix", suffix);
      return components;
    }
}
