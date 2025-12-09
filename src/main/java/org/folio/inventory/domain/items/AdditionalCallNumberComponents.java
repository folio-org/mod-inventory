package org.folio.inventory.domain.items;

import io.vertx.core.json.JsonObject;

public class AdditionalCallNumberComponents {
    private final String additionalCallNumber;
    private final String additionalCallNumberPrefix;
    private final String additionalCallNumberSuffix;
    private final String additionalCallNumberTypeId;

    public AdditionalCallNumberComponents(String callNumber, String prefix,
                                         String suffix, String typeId) {
      this.additionalCallNumber = callNumber;
      this.additionalCallNumberPrefix = prefix;
      this.additionalCallNumberSuffix = suffix;
      this.additionalCallNumberTypeId = typeId;
    }

  public String getAdditionalCallNumber() {
    return additionalCallNumber;
  }

  public String getAdditionalCallNumberPrefix() {
    return additionalCallNumberPrefix;
  }

  public String getAdditionalCallNumberSuffix() {
    return additionalCallNumberSuffix;
  }

  public String getAdditionalCallNumberTypeId() {
    return additionalCallNumberTypeId;
  }

  public static AdditionalCallNumberComponents from(JsonObject representation) {
      if (representation == null) {
        return null;
      }

      return new AdditionalCallNumberComponents(
        representation.getString("additionalCallNumber"),
        representation.getString("additionalCallNumberPrefix"),
        representation.getString("additionalCallNumberSuffix"),
        representation.getString("additionalCallNumberTypeId"));
    }

    public JsonObject toJson() {
      JsonObject components = new JsonObject();
      components.put("additionalCallNumber", additionalCallNumber);
      components.put("additionalCallNumberPrefix", additionalCallNumberPrefix);
      components.put("additionalCallNumberSuffix", additionalCallNumberSuffix);
      components.put("additionalCallNumberTypeId", additionalCallNumberTypeId);
      return components;
    }
}
