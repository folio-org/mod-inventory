package org.folio.inventory.domain.instances;

import io.vertx.core.json.JsonObject;

public class Subject {
  // JSON property names
  public static final String VALUE_KEY = "value";
  public static final String AUTHORITY_ID_KEY = "authorityId";

  private final String value;
  private final String authorityId;

  public Subject(String value, String authorityId) {
    this.value = value;
    this.authorityId = authorityId;
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
