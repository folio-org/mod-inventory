package org.folio.inventory.domain.user;

import io.vertx.core.json.JsonObject;

public record Personal(String lastName, String firstName) {

  public static final String LAST_NAME_KEY = "lastName";
  public static final String FIRST_NAME_KEY = "firstName";

  public Personal(JsonObject json) {
    this(json.getString(LAST_NAME_KEY), json.getString(FIRST_NAME_KEY));
  }
}
