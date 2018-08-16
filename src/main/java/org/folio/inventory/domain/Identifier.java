package org.folio.inventory.domain;

import io.vertx.core.json.JsonObject;

public class Identifier {
  public static final String IDENTIFIER_TYPE_ID = "identifierTypeId";
  public static final String VALUE = "value";

  public final String identifierTypeId;
  public final String value;

  public Identifier(String identifierTypeId, String value) {
    this.identifierTypeId = identifierTypeId;
    this.value = value;
  }

  public Identifier (JsonObject json) {
    this(json.getString(IDENTIFIER_TYPE_ID), json.getString(VALUE));
  }

}
