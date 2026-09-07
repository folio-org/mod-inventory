package org.folio.inventory.domain;

import io.vertx.core.json.JsonObject;

public record Metadata(String createdDate, String createdByUserId, String updatedDate, String updatedByUserId) {
  // JSON property names
  public static final String CREATED_DATE_KEY = "createdDate";
  public static final String CREATED_BY_USER_ID_KEY = "createdByUserId";
  public static final String UPDATED_DATE_KEY = "updatedDate";
  public static final String UPDATED_BY_USER_ID_KEY = "updatedByUserId";

  public Metadata(JsonObject json) {
    if (json != null) {
      this.createdDate = json.getString(CREATED_DATE_KEY);
      this.createdByUserId = json.getString(CREATED_BY_USER_ID_KEY);
      this.updatedDate = json.getString(UPDATED_DATE_KEY);
      this.updatedByUserId = json.getString(UPDATED_BY_USER_ID_KEY);
    } else {
      this.createdDate = null;
      this.createdByUserId = null;
      this.updatedDate = null;
      this.updatedByUserId = null;
    }
  }
}
