package org.folio.inventory.domain.instances;

import io.vertx.core.json.JsonObject;

public record Identifier(String identifierTypeId, String value) {
  public static final String IDENTIFIER_TYPE_ID_KEY = "identifierTypeId";
  public static final String VALUE_KEY = "value";

  public Identifier(JsonObject json) {
    this(json.getString(IDENTIFIER_TYPE_ID_KEY), json.getString(VALUE_KEY));
  }
}
