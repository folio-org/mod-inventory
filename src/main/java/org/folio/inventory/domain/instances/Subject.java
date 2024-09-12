package org.folio.inventory.domain.instances;

import io.vertx.core.json.JsonObject;

public class Subject extends Authorized {
  // JSON property names
  private static final String VALUE_KEY = "value";
  private static final String SOURCE_KEY = "sourceId";
  private static final String TYPE_KEY = "typeId";

  private final String value;

  private final String sourceId;

  private final String typeId;

  public Subject(String value, String authorityId, String sourceId, String typeId) {
    super(authorityId);
    this.value = value;
    this.sourceId = sourceId;
    this.typeId = typeId;
  }

  public Subject(JsonObject json) {
    this(
      json.getString(VALUE_KEY),
      json.getString(AUTHORITY_ID_KEY),
      json.getString(SOURCE_KEY),
      json.getString(TYPE_KEY)
    );
  }

  public String getAuthorityId() {
    return authorityId;
  }

  public String getValue() {
    return value;
  }

  public String getSourceId() {
    return sourceId;
  }

  public String getTypeId() {
    return typeId;
  }
}
