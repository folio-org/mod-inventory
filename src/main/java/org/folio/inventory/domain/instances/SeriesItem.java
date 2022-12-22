package org.folio.inventory.domain.instances;

import io.vertx.core.json.JsonObject;

public class SeriesItem {
  // JSON property names
  public static final String VALUE_KEY = "value";
  public static final String AUTHORITY_ID_KEY = "authorityId";

  public final String value;
  public final String authorityId;

  public SeriesItem(String value, String authorityId) {
    this.value = value;
    this.authorityId = authorityId;
  }

  public SeriesItem(JsonObject json) {
    this(json.getString(VALUE_KEY), json.getString(AUTHORITY_ID_KEY));
  }

}
