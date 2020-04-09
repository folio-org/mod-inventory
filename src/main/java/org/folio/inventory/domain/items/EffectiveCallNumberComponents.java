package org.folio.inventory.domain.items;

import io.vertx.core.json.JsonObject;

public class EffectiveCallNumberComponents {
    private String callNumber;
    private String prefix;
    private String suffix;
    private String typeId;

    public EffectiveCallNumberComponents(String callNumber, String prefix,
                                         String suffix, String typeId) {
      this.callNumber = callNumber;
      this.prefix = prefix;
      this.suffix = suffix;
      this.typeId = typeId;
    }

  public String getCallNumber() {
    return callNumber;
  }

  public String getPrefix() {
    return prefix;
  }

  public String getSuffix() {
    return suffix;
  }

  public String getTypeId() {
    return typeId;
  }

  public static EffectiveCallNumberComponents from(JsonObject representation) {
      if (representation == null) {
        return null;
      }

      return new EffectiveCallNumberComponents(
        representation.getString("callNumber"),
        representation.getString("prefix"),
        representation.getString("suffix"),
        representation.getString("typeId"));
    }

    public JsonObject toJson() {
      JsonObject components = new JsonObject();
      components.put("callNumber", callNumber);
      components.put("prefix", prefix);
      components.put("suffix", suffix);
      components.put("typeId", typeId);
      return components;
    }
}
