package org.folio.inventory.domain.instances;

import io.vertx.core.json.JsonObject;

public record Classification(String classificationTypeId, String classificationNumber) {
  // JSON property names
  public static final String CLASSIFICATION_NUMBER_KEY = "classificationNumber";
  public static final String CLASSIFICATION_TYPE_ID_KEY = "classificationTypeId";

  public Classification(JsonObject json) {
    this(json.getString(CLASSIFICATION_TYPE_ID_KEY),
      json.getString(CLASSIFICATION_NUMBER_KEY));
  }
}
