package org.folio.inventory.domain.instances;

import io.vertx.core.json.JsonObject;

public class SeriesItem extends Authorized {
  // JSON property names
  public static final String VALUE_KEY = "value";

  public final String value;

  public SeriesItem(String value, String authorityId) {
    super(authorityId);
    this.value = value;
  }

  public SeriesItem(JsonObject json) {
    this(json.getString(VALUE_KEY), json.getString(AUTHORITY_ID_KEY));
  }

}
