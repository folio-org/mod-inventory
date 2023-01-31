package org.folio.inventory.domain.instances;

import io.vertx.core.json.JsonObject;

public class Subject extends Authorized {
  // JSON property names
  public static final String VALUE_KEY = "value";

  private final String value;

  public Subject(String value, String authorityId) {
    super(authorityId);
    this.value = value;
  }

  public Subject(JsonObject json) {
    this(json.getString(VALUE_KEY), json.getString(AUTHORITY_ID_KEY));
  }

  public String getAuthorityId() {
    return authorityId;
  }

  public String getValue() {
    return value;
  }
}
