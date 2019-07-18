package org.folio.inventory.domain.user;

import io.vertx.core.json.JsonObject;

public class User {

  public static final String ID_KEY = "id";
  public static final String PERSONAL_KEY = "personal";

  private String id;
  private Personal personal;

  public User(String id, Personal personal) {
    this.id = id;
    this.personal = personal;
  }

  public User(JsonObject json) {
    this(json != null ? json.getString(ID_KEY) : null,
      json != null ? new Personal(json.getJsonObject(PERSONAL_KEY)) : null);
  }

  public String getId() {
    return id;
  }

  public Personal getPersonal() {
    return personal;
  }
}
