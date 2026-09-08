package org.folio.inventory.domain.user;

import io.vertx.core.json.JsonObject;

public record User(String id, Personal personal) {

  public static final String ID_KEY = "id";
  public static final String PERSONAL_KEY = "personal";

  public User(JsonObject json) {
    this(json != null && json.getString(ID_KEY) != null ? json.getString(ID_KEY) : null,
      json != null && json.getJsonObject(PERSONAL_KEY) != null ? new Personal(json.getJsonObject(PERSONAL_KEY)) : null);
  }
}
